// Package marketregistry exposes the immutable public subset of the matching
// market registry. Never derive numeric IDs from untrusted HTTP parameters.
package marketregistry

import (
	"errors"
	"fmt"

	"github.com/MeowMeowOrganization/meow-market-data/internal/recovery"
)

type Market struct {
	Symbol            string
	SourceID          uint32
	DefinitionVersion uint64
	Depth             uint32
}

type Registry struct {
	bySymbol map[string]Market
	byID     map[uint32]Market
}

func New(configs []recovery.MarketConfig) (*Registry, error) {
	if len(configs) == 0 {
		return nil, errors.New("market registry: no markets")
	}
	r := &Registry{bySymbol: map[string]Market{}, byID: map[uint32]Market{}}
	for _, cfg := range configs {
		if cfg.Definition == nil || cfg.Definition.GetMarket() == nil {
			return nil, errors.New("market registry: nil definition")
		}
		m := Market{Symbol: cfg.Definition.GetMarket().GetMarketId(), SourceID: cfg.Definition.GetSourceMarketId(), DefinitionVersion: cfg.Definition.GetDefinitionVersion(), Depth: cfg.Depth}
		if m.Symbol == "" || m.DefinitionVersion == 0 || m.Depth == 0 {
			return nil, fmt.Errorf("market registry: invalid market: %+v", m)
		}
		if _, exists := r.bySymbol[m.Symbol]; exists {
			return nil, fmt.Errorf("market registry: duplicate symbol %q", m.Symbol)
		}
		if _, exists := r.byID[m.SourceID]; exists {
			return nil, fmt.Errorf("market registry: duplicate ID %d", m.SourceID)
		}
		r.bySymbol[m.Symbol] = m
		r.byID[m.SourceID] = m
	}
	return r, nil
}
func (r *Registry) BySymbol(s string) (Market, bool) {
	if r == nil {
		return Market{}, false
	}
	m, ok := r.bySymbol[s]
	return m, ok
}
func (r *Registry) ByID(id uint32) (Market, bool) {
	if r == nil {
		return Market{}, false
	}
	m, ok := r.byID[id]
	return m, ok
}
