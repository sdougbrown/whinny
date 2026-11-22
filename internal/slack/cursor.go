package slack

import (
	"fmt"
	"strconv"
	"strings"
)

// TSToCursor converts a Slack ts ("1234567890.123456") to a cursor:
// epoch seconds and the fraction, in microseconds. The fraction is
// right-padded to 6 digits (or truncated past 6), which keeps the
// per-channel ordering Slack guarantees.
func TSToCursor(ts string) int64 {
	sec, frac, _ := strings.Cut(ts, ".")
	s, err := strconv.ParseInt(sec, 10, 64)
	if err != nil {
		return 0
	}
	frac = (frac + "000000")[:6]
	f, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return s * 1_000_000
	}
	return s*1_000_000 + f
}

// CursorToTS converts a cursor back to a Slack ts string.
func CursorToTS(c int64) string {
	if c <= 0 {
		return ""
	}
	return fmt.Sprintf("%d.%06d", c/1_000_000, c%1_000_000)
}
