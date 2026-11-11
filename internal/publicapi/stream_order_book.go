package publicapi

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	api "github.com/MeowMeowOrganization/meow-contracts/gen/go/meow/exchange/api/v1alpha1"
)

// StreamOrderBook is part of the same public MarketDataService as the unary
// methods, but delegates transport lifecycle and sequencing to stream.BookRPC.
func (s *Service) StreamOrderBook(ctx context.Context, request *connect.Request[api.StreamOrderBookRequest], output *connect.ServerStream[api.StreamOrderBookResponse]) error {
	if s.BookStream == nil {
		return connect.NewError(connect.CodeUnavailable, errors.New("book streaming is not configured"))
	}
	return s.BookStream.StreamOrderBook(ctx, request, output)
}
