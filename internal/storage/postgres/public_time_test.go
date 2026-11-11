package postgres

import (
	"testing"
	"time"
)

func TestParseNanosDecimalExact(t *testing.T) {
	for _, stamp := range []time.Time{
		time.Date(2026, 11, 10, 12, 0, 0, 123456789, time.UTC),
		time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC),
	} {
		got, err := parseNanosDecimal(NanosDecimal(stamp))
		if err != nil || !got.Equal(stamp) {
			t.Fatalf("nanosecond precision failed: got=%v want=%v err=%v", got, stamp, err)
		}
	}
	for _, bad := range []string{"", "garbage", "100000000000000000000000000000000000", "+123", "001"} {
		if _, err := parseNanosDecimal(bad); err == nil {
			t.Fatalf("accepted non-canonical timestamp %q", bad)
		}
	}
}
