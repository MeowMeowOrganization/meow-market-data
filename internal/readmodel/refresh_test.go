package readmodel

import (
	"context"
	"testing"
	"time"
)

func TestRefreshRejectsIncompleteDependency(t *testing.T) {
	if err := RefreshAll(context.Background(), nil, nil, nil, time.Now()); err == nil {
		t.Fatal("nil dependencies accepted")
	}
}
