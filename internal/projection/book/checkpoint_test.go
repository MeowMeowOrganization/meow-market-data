package book

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestCheckpointRestoresFullRestingOrderIndex(t *testing.T) {
	original := newBook(t, 1)
	apply(t, original, committed(1, place(10, buy, 400, 5)))
	apply(t, original, committed(2, place(11, buy, 399, 7))) // hidden by depth
	apply(t, original, committed(3, place(12, buy, 400, 2))) // same price, distinct order
	blob, err := original.MarshalCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := Restore(definition(), 1, blob)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(original.Snapshot(), restored.Snapshot()) || restored.Sequence() != 3 {
		t.Fatal("restored snapshot does not equal original")
	}
	d2, err := restored.MarshalCheckpoint()
	if err != nil || !bytes.Equal(blob, d2) {
		t.Fatalf("checkpoint roundtrip failed %v", err)
	}
	// An identical replay of the latest result is still a no-op.
	dup, err := restored.Apply(committed(3, place(12, buy, 400, 2)))
	if err != nil || dup != nil {
		t.Fatalf("latest replay = %v, %v", dup, err)
	}
	// Filling the hidden maker must work. An L2-only restore would fail here.
	apply(t, restored, committed(4, place(13, sell, 398, 14),
		execution(10, 13, sell, 400, 5),
		execution(12, 13, sell, 400, 2),
		execution(11, 13, sell, 399, 7)))
	if len(restored.Snapshot().GetBids()) != 0 {
		t.Fatal("restored maker index did not clear")
	}
}

func TestRestoreRejectsCorruptOrIncompatibleSnapshot(t *testing.T) {
	p := newBook(t, 3)
	apply(t, p, committed(1, place(10, buy, 400, 5)))
	blob, err := p.MarshalCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(map[string]any){
		"definition":      func(m map[string]any) { m["definition_version"] = float64(99) },
		"depth":           func(m map[string]any) { m["depth"] = float64(4) },
		"digest":          func(m map[string]any) { m["last_envelope_sha256"] = "x" },
		"duplicate order": func(m map[string]any) { orders := m["orders"].([]any); m["orders"] = append(orders, orders[0]) },
		"negative lots":   func(m map[string]any) { m["orders"].([]any)[0].(map[string]any)["remaining_lots"] = float64(-1) },
	} {
		t.Run(name, func(t *testing.T) {
			var m map[string]any
			if err := json.Unmarshal(blob, &m); err != nil {
				t.Fatal(err)
			}
			change(m)
			invalid, err := json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			_, err = Restore(definition(), 3, invalid)
			if !errors.Is(err, ErrInvalidCheckpoint) {
				t.Fatalf("expected invalid checkpoint, got %v", err)
			}
		})
	}
}
