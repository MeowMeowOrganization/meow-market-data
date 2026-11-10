package stream

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	marketv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/market/v1alpha1"
	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/marketregistry"
	"github.com/MeowMeowOrganization/meow-market-data/internal/recovery"
	"github.com/gorilla/websocket"
)

func wsRegistry(t *testing.T) *marketregistry.Registry {
	t.Helper()
	r, e := marketregistry.New([]recovery.MarketConfig{{PartitionID: 0, Depth: 20, Definition: &md.MarketProjectionDefinition{
		SourceMarketId: 7, DefinitionVersion: 3, Market: &marketv1.Market{MarketId: "BTC-USDT"}}}})
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func readFrame(t *testing.T, ws *websocket.Conn) map[string]any {
	t.Helper()
	_ = ws.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, data, e := ws.ReadMessage()
	if e != nil {
		t.Fatal(e)
	}
	var frame map[string]any
	if e = json.Unmarshal(data, &frame); e != nil {
		t.Fatalf("invalid JSON WS frame: %s: %v", data, e)
	}
	return frame
}
func TestWebSocketSnapshotThenSequencedReplay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &fakeSource{snapshotSeq: 1, committed: []Result{mkResult(2)}}
	hub, _ := NewHub(ctx, src, 50*time.Millisecond, time.Second, 32)
	handler, e := NewHandler(wsRegistry(t), src, hub, nil, 2)
	if e != nil {
		t.Fatal(e)
	}
	handler.Shutdown = ctx.Done()
	server := httptest.NewServer(handler)
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "?market_id=BTC-USDT&channels=book,trades,candles"
	ws, _, e := websocket.DefaultDialer.Dial(wsURL, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer ws.Close()
	if f := readFrame(t, ws); f["type"] != "ready" || f["mode"] != "snapshot" {
		t.Fatalf("unexpected ready frame: %+v", f)
	}
	if f := readFrame(t, ws); f["type"] != "update" {
		t.Fatalf("missing book snapshot: %+v", f)
	}
	if f := readFrame(t, ws); f["type"] != "sequence_end" || f["marketSequence"] != "1" {
		t.Fatalf("missing complete snapshot cursor: %+v", f)
	}
	if f := readFrame(t, ws); f["type"] != "update" {
		t.Fatalf("missing book delta: %+v", f)
	}
	if f := readFrame(t, ws); f["type"] != "sequence_end" || f["marketSequence"] != "2" {
		t.Fatalf("missing sequence end: %+v", f)
	}
}
func TestWebSocketResumeAndCrossOriginRejection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &fakeSource{snapshotSeq: 2, committed: []Result{mkResult(2)}}
	hub, _ := NewHub(ctx, src, 50*time.Millisecond, time.Second, 32)
	handler, _ := NewHandler(wsRegistry(t), src, hub, nil, 2)
	handler.Shutdown = ctx.Done()
	server := httptest.NewServer(handler)
	defer server.Close()
	path := "ws" + strings.TrimPrefix(server.URL, "http") + "?market_id=BTC-USDT&after_sequence=1&market_definition_version=3"
	_, response, err := websocket.DefaultDialer.Dial(path, http.Header{"Origin": []string{"https://attacker.example"}})
	if err == nil || response == nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("cross origin accepted: %v %+v", err, response)
	}
	ws, _, err := websocket.DefaultDialer.Dial(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	if f := readFrame(t, ws); f["mode"] != "replay" {
		t.Fatalf("resume did not use durable journal: %+v", f)
	}
	if f := readFrame(t, ws); f["type"] != "update" {
		t.Fatalf("missing replayed book delta: %+v", f)
	}
	if f := readFrame(t, ws); f["type"] != "sequence_end" || f["marketSequence"] != "2" {
		t.Fatalf("resume cursor: %+v", f)
	}
}

func TestWebSocketResumeBelowMigrationFloorResets(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &fakeSource{snapshotSeq: 10, floor: 9}
	hub, _ := NewHub(ctx, src, 50*time.Millisecond, time.Second, 32)
	handler, _ := NewHandler(wsRegistry(t), src, hub, nil, 2)
	handler.Shutdown = ctx.Done()
	server := httptest.NewServer(handler)
	defer server.Close()
	url := "ws" + strings.TrimPrefix(server.URL, "http") + "?market_id=BTC-USDT&after_sequence=3&market_definition_version=3"
	ws, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	if f := readFrame(t, ws); f["mode"] != "snapshot" {
		t.Fatalf("must reset before recorded candle revision history: %+v", f)
	}
	if f := readFrame(t, ws); f["type"] != "update" {
		t.Fatalf("fresh snapshot missing: %+v", f)
	}
}
