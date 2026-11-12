package bootstrap

import "testing"

func TestTradeStreamCapacityConfiguration(t *testing.T) {
	configureMarketDataTestEnv(t)
	t.Setenv("MARKET_DATA_HTTP_MAX_TRADE_RPC_STREAMS", "")
	cfg, err := LoadFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxTradeRPCStreams != 256 {
		t.Fatalf("default trade cap: %d", cfg.MaxTradeRPCStreams)
	}
	t.Setenv("MARKET_DATA_HTTP_MAX_TRADE_RPC_STREAMS", "16")
	cfg, err = LoadFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxTradeRPCStreams != 16 {
		t.Fatalf("overridden cap: %d", cfg.MaxTradeRPCStreams)
	}
}
func TestTradeStreamCapacityRejectsInvalidValues(t *testing.T) {
	for _, invalid := range []string{"0", "-1", "10001", "not-an-integer"} {
		t.Run(invalid, func(t *testing.T) {
			configureMarketDataTestEnv(t)
			t.Setenv("MARKET_DATA_HTTP_MAX_TRADE_RPC_STREAMS", invalid)
			if _, err := LoadFromEnvironment(); err == nil {
				t.Fatal("unsafe trade stream limit accepted")
			}
		})
	}
}
