package publicapi

import (
	"connectrpc.com/connect"
	"context"
	"errors"
	api "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/api/v1alpha1"
)

// StreamTrades deliberately exposes public executions only. It carries no
// private order status, account identifier, or settlement state.
func (s *Service) StreamTrades(ctx context.Context, request *connect.Request[api.StreamTradesRequest], output *connect.ServerStream[api.StreamTradesResponse]) error {
	if s.TradeStream == nil {
		return connect.NewError(connect.CodeUnavailable, errors.New("trade streaming is not configured"))
	}
	return s.TradeStream.StreamTrades(ctx, request, output)
}
