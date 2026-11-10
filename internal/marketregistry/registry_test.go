package marketregistry

import (
	marketv1 "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/market/v1alpha1"
	md "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/marketdata/v1alpha1"
	"github.com/MeowMeowOrganization/meow-market-data/internal/recovery"
	"testing"
)

func TestRegistryRejectsDuplicateSymbolsAndIDs(t *testing.T) {
	makeCfg := func(symbol string, id uint32) recovery.MarketConfig {
		return recovery.MarketConfig{Depth: 10, Definition: &md.MarketProjectionDefinition{SourceMarketId: id,
			DefinitionVersion: 3, Market: &marketv1.Market{MarketId: symbol}}}
	}
	r, e := New([]recovery.MarketConfig{makeCfg("BTC-USDT", 7)})
	if e != nil {
		t.Fatal(e)
	}
	if got, ok := r.BySymbol("BTC-USDT"); !ok || got.SourceID != 7 {
		t.Fatalf("symbol lookup: %+v %v", got, ok)
	}
	if _, e = New([]recovery.MarketConfig{makeCfg("BTC-USDT", 7), makeCfg("BTC-USDT", 8)}); e == nil {
		t.Fatal("duplicate symbol accepted")
	}
	if _, e = New([]recovery.MarketConfig{makeCfg("BTC-USDT", 7), makeCfg("ETH-USDT", 7)}); e == nil {
		t.Fatal("duplicate numeric ID accepted")
	}
}
