package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	writeTimeout = 5 * time.Second
	pongWait     = 60 * time.Second
	pingEvery    = 20 * time.Second
	maxReplay    = 20000 // bounded catch-up; larger gaps reset to snapshot
)

type Handler struct {
	Registry       *marketregistry.Registry
	Source         Source
	Hub            *Hub
	Shutdown       <-chan struct{} // application cancellation, including hijacked WS connections
	AllowedOrigins []string
	connections    chan struct{}
}

func NewHandler(registry *marketregistry.Registry, source Source, hub *Hub, origins []string, maxConnections int) (*Handler, error) {
	if registry == nil || source == nil || hub == nil || maxConnections < 1 || maxConnections > 10000 {
		return nil, errors.New("stream: invalid server configuration")
	}
	for _, origin := range origins {
		parsed, err := url.Parse(origin)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "" {
			return nil, fmt.Errorf("stream: invalid allowed origin %q", origin)
		}
	}
	return &Handler{Registry: registry, Source: source, Hub: hub, AllowedOrigins: append([]string(nil), origins...), connections: make(chan struct{}, maxConnections)}, nil
}
func (h *Handler) originOK(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	} // non-browser clients; restrict service to internal network
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Host == "" || parsed.User != nil {
		return false
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return false
	}
	for _, allowed := range h.AllowedOrigins {
		if origin == allowed {
			return true
		}
	}
	// Browser clients behind the Gateway normally use same-origin transport.
	if !strings.EqualFold(parsed.Host, r.Host) {
		return false
	}
	if r.TLS != nil {
		return parsed.Scheme == "https"
	}
	return parsed.Scheme == "http"
}
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	symbol := r.URL.Query().Get("market_id")
	m, ok := h.Registry.BySymbol(symbol)
	if !ok {
		http.Error(w, "unknown market", http.StatusNotFound)
		return
	}
	if !h.originOK(r) {
		http.Error(w, "forbidden origin", http.StatusForbidden)
		return
	}
	depth := m.Depth
	if raw := r.URL.Query().Get("depth"); raw != "" {
		n, err := strconv.ParseUint(raw, 10, 32)
		if err != nil || uint32(n) != m.Depth {
			http.Error(w, "stream depth must equal materialized book depth", http.StatusBadRequest)
			return
		}
		depth = uint32(n)
	}
	channels, err := parseChannels(r.URL.Query().Get("channels"))
	if err != nil {
		http.Error(w, "invalid channels", http.StatusBadRequest)
		return
	}
	var after uint64
	resume := false
	if raw := r.URL.Query().Get("after_sequence"); raw != "" {
		n, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || n == 0 || n == math.MaxUint64 {
			http.Error(w, "invalid reconnect sequence", http.StatusBadRequest)
			return
		}
		after = n
		resume = true
	}
	var version uint64
	if resume {
		n, err := strconv.ParseUint(r.URL.Query().Get("market_definition_version"), 10, 64)
		if err != nil || n == 0 {
			http.Error(w, "market_definition_version required for resume", http.StatusBadRequest)
			return
		}
		version = n
	}
	select {
	case h.connections <- struct{}{}:
		defer func() { <-h.connections }()
	default:
		http.Error(w, "stream capacity exceeded", http.StatusServiceUnavailable)
		return
	}

	// Subscribe before reading the PG snapshot. Buffered hub messages can
	// never vanish between snapshot acquisition and live delivery.
	ctx := r.Context()
	sub, err := h.Hub.Subscribe(ctx, m)
	if err != nil {
		http.Error(w, "market stream not ready", http.StatusServiceUnavailable)
		return
	}
	defer sub.Close()
	snap, err := h.Source.Snapshot(ctx, m)
	if err != nil {
		http.Error(w, "market stream snapshot unavailable", http.StatusServiceUnavailable)
		return
	}
	floor, err := h.Source.ReplayFloor(ctx, m)
	if err != nil {
		http.Error(w, "market stream recovery unavailable", http.StatusServiceUnavailable)
		return
	}
	u := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }, EnableCompression: false, ReadBufferSize: 4096, WriteBufferSize: 8192}
	ws, err := u.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer ws.Close()
	ws.SetReadLimit(4096)
	_ = ws.SetReadDeadline(time.Now().Add(pongWait))
	ws.SetPongHandler(func(string) error { return ws.SetReadDeadline(time.Now().Add(pongWait)) })
	readErrors := make(chan error, 1)
	go func() {
		for {
			_, _, e := ws.ReadMessage()
			if e != nil {
				readErrors <- e
				return
			} else {
				readErrors <- errors.New("client application frames not supported")
				return
			}
		}
	}()

	cursor := uint64(0)
	canReplay := resume && version == m.DefinitionVersion && after <= snap.GetMarketSequence() &&
		after >= floor-1 && snap.GetMarketSequence()-after <= maxReplay
	if canReplay {
		if err = h.writeControl(ws, map[string]any{"type": "ready", "mode": "replay", "marketId": m.Symbol,
			"depth": depth, "marketDefinitionVersion": strconv.FormatUint(m.DefinitionVersion, 10),
			"fromSequence": strconv.FormatUint(after, 10), "headSequence": strconv.FormatUint(snap.GetMarketSequence(), 10)}); err != nil {
			return
		}
		cursor = after
		for cursor < snap.GetMarketSequence() {
			queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			results, e := h.Source.Results(queryCtx, m, cursor, snap.GetMarketSequence(), 256)
			cancel()
			if e != nil || len(results) == 0 {
				h.resync(ws)
				return
			}
			for i := range results {
				if e = h.sendResult(ws, &cursor, results[i], channels); e != nil {
					h.resync(ws)
					return
				}
			}
		}
	} else {
		if err = h.writeControl(ws, map[string]any{"type": "ready", "mode": "snapshot", "marketId": m.Symbol,
			"depth": depth, "marketDefinitionVersion": strconv.FormatUint(m.DefinitionVersion, 10),
			"headSequence": strconv.FormatUint(snap.GetMarketSequence(), 10)}); err != nil {
			return
		}
		if channels["book"] {
			if err = h.writeUpdate(ws, &md.MarketDataUpdate{Update: &md.MarketDataUpdate_BookSnapshot{BookSnapshot: snap}}); err != nil {
				return
			}
		}
		cursor = snap.GetMarketSequence()
		// A client subscribing only to trades still gets a cursor handshake.
		if err = h.writeControl(ws, map[string]any{"type": "sequence_end", "marketSequence": strconv.FormatUint(cursor, 10)}); err != nil {
			return
		}
	}
	// Latest nonsequenced stats/ticker are advisory snapshots; neither advances
	// the durable cursor. Candle updates are sequenced in candle_updates.
	if channels["ticker"] || channels["stats"] {
		queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		soft, e := h.Source.Current(queryCtx, m, time.Now().UTC())
		cancel()
		if e == nil && soft != nil {
			if e = h.sendSoft(ws, soft, channels); e != nil {
				return
			}
		}
	}
	ping := time.NewTicker(pingEvery)
	defer ping.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.Shutdown:
			return
		case <-readErrors:
			return
		case e := <-sub.Errors:
			_ = e
			h.resync(ws)
			return
		case msg := <-sub.Messages:
			if msg.Result != nil {
				if err = h.sendResult(ws, &cursor, *msg.Result, channels); err != nil {
					h.resync(ws)
					return
				}
			}
			if msg.Soft != nil {
				if err = h.sendSoft(ws, msg.Soft, channels); err != nil {
					return
				}
			}
		case <-ping.C:
			if err = ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeTimeout)); err != nil {
				return
			}
		}
	}
}
func parseChannels(raw string) (map[string]bool, error) {
	supported := map[string]bool{"book": true, "trades": true, "candles": true, "ticker": true, "stats": true}
	if raw == "" {
		return supported, nil
	}
	out := map[string]bool{}
	for _, channel := range strings.Split(raw, ",") {
		if !supported[channel] {
			return nil, errors.New("unsupported channel")
		}
		out[channel] = true
	}
	if len(out) == 0 {
		return nil, errors.New("no channels")
	}
	return out, nil
}
func (h *Handler) sendResult(ws *websocket.Conn, cursor *uint64, r Result, channels map[string]bool) error {
	if r.Delta == nil {
		return ErrSequenceGap
	}
	seq := r.Delta.GetMarketSequence()
	if seq <= *cursor {
		return nil
	} // already present in initial snapshot/replay
	if seq-*cursor != 1 {
		return ErrSequenceGap
	}
	if channels["book"] {
		if err := h.writeUpdate(ws, &md.MarketDataUpdate{Update: &md.MarketDataUpdate_BookDelta{BookDelta: r.Delta}}); err != nil {
			return err
		}
	}
	if channels["trades"] {
		for _, t := range r.Trades {
			if err := h.writeUpdate(ws, &md.MarketDataUpdate{Update: &md.MarketDataUpdate_Trade{Trade: t}}); err != nil {
				return err
			}
		}
	}
	if channels["candles"] {
		for _, c := range r.Candles {
			if err := h.writeUpdate(ws, &md.MarketDataUpdate{Update: &md.MarketDataUpdate_Candle{Candle: c}}); err != nil {
				return err
			}
		}
	}
	if err := h.writeControl(ws, map[string]any{"type": "sequence_end", "marketSequence": strconv.FormatUint(seq, 10)}); err != nil {
		return err
	}
	*cursor = seq
	return nil
}
func (h *Handler) sendSoft(ws *websocket.Conn, soft *Soft, channels map[string]bool) error {
	if channels["ticker"] && soft.Ticker != nil {
		if err := h.writeUpdate(ws, &md.MarketDataUpdate{Update: &md.MarketDataUpdate_Ticker{Ticker: soft.Ticker}}); err != nil {
			return err
		}
	}
	if channels["stats"] && soft.Stats != nil {
		// The proto field is named stats_24h (underscore before a digit);
		// setting it reflectively avoids depending on generator-specific
		// CamelCase naming for this unusual oneof field.
		update := new(md.MarketDataUpdate)
		field := update.ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name("stats_24h"))
		if field == nil {
			return errors.New("stream: stats_24h contract field missing")
		}
		update.ProtoReflect().Set(field, protoreflect.ValueOfMessage(soft.Stats.ProtoReflect()))
		if err := h.writeUpdate(ws, update); err != nil {
			return err
		}
	}
	return nil
}
func (h *Handler) writeUpdate(ws *websocket.Conn, v *md.MarketDataUpdate) error {
	raw, err := protojson.Marshal(v)
	if err != nil {
		return err
	}
	payload := append([]byte(`{"type":"update","data":`), raw...)
	payload = append(payload, '}')
	if err := ws.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return err
	}
	return ws.WriteMessage(websocket.TextMessage, payload)
}
func (h *Handler) writeControl(ws *websocket.Conn, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if err := ws.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return err
	}
	return ws.WriteMessage(websocket.TextMessage, raw)
}
func (h *Handler) resync(ws *websocket.Conn) {
	_ = h.writeControl(ws, map[string]string{"type": "resync_required", "reason": "projection continuity or subscriber capacity lost"})
	_ = ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "resync required"), time.Now().Add(writeTimeout))
}
