package bootstrap

import "testing"

func TestTickerStreamCapacityConfiguration(t *testing.T) {
	configureMarketDataTestEnv(t)
	t.Setenv("MARKET_DATA_HTTP_MAX_TICKER_RPC_STREAMS", "")
	cfg, err := LoadFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxTickerRPCStreams != 256 {
		t.Fatalf("default ticker cap: %d", cfg.MaxTickerRPCStreams)
	}
	t.Setenv("MARKET_DATA_HTTP_MAX_TICKER_RPC_STREAMS", "12")
	cfg, err = LoadFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxTickerRPCStreams != 12 {
		t.Fatalf("overridden cap: %d", cfg.MaxTickerRPCStreams)
	}
}

func TestTickerStreamCapacityRejectsInvalidValues(t *testing.T) {
	for _, bad := range []string{"0", "-1", "10001", "NaN"} {
		t.Run(bad, func(t *testing.T) {
			configureMarketDataTestEnv(t)
			t.Setenv("MARKET_DATA_HTTP_MAX_TICKER_RPC_STREAMS", bad)
			if _, err := LoadFromEnvironment(); err == nil {
				t.Fatal("accepted unsafe ticker stream limit")
			}
		})
	}
}
