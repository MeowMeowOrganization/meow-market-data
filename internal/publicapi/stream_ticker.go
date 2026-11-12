package publicapi

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	api "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/api/v1alpha1"
)

// StreamTicker exposes a public, replaceable latest-state market ticker.
// It has no account identity, order state, or historical execution replay.
func (s *Service) StreamTicker(ctx context.Context, request *connect.Request[api.StreamTickerRequest], output *connect.ServerStream[api.StreamTickerResponse]) error {
	if s.TickerStream == nil {
		return connect.NewError(connect.CodeUnavailable, errors.New("ticker streaming is not configured"))
	}
	return s.TickerStream.StreamTicker(ctx, request, output)
}
