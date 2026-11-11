package redis

import "testing"

func TestNamespacedCacheKeys(t *testing.T) {
	first, err := NewNamespace("matching.results.v1", "1")
	if err != nil {
		t.Fatal(err)
	}
	again, err := NewNamespace("matching.results.v1", "1")
	if err != nil {
		t.Fatal(err)
	}
	otherTopic, err := NewNamespace("matching.results.v2", "1")
	if err != nil {
		t.Fatal(err)
	}
	otherEpoch, err := NewNamespace("matching.results.v1", "2")
	if err != nil {
		t.Fatal(err)
	}
	prefix := first.MarketPrefix(7, 3)
	if prefix == "" || prefix != again.MarketPrefix(7, 3) {
		t.Fatal("namespace is not reproducible")
	}
	if prefix == otherTopic.MarketPrefix(7, 3) || prefix == otherEpoch.MarketPrefix(7, 3) || prefix == first.MarketPrefix(7, 4) {
		t.Fatal("cache generations and market definitions must be physically isolated")
	}
	if first.Key(7, 3, "book:top") != prefix+"book:top" {
		t.Fatal("invalid prefix")
	}
	if _, err := NewNamespace("", "1"); err == nil {
		t.Fatal("empty topic accepted")
	}
	if _, err := NewNamespace("topic", ""); err == nil {
		t.Fatal("empty generation accepted")
	}
}
