package cli

import (
	"context"
	"io"
	"strconv"
	"time"

	"github.com/sdougbrown/cli-contract"
)

// MetaQuietUntil is the store meta key for the send-path quiet gate:
// a unix-seconds timestamp after which posts are allowed again. An
// empty value (or an unset key) means not quiet.
const MetaQuietUntil = "quiet_until"

// QuietUntilIndefinite is the quiet_until value for an indefinite
// quiet (--for 0): far enough in the future (year 2286) to read as
// "never".
const QuietUntilIndefinite = 9999999999

// DefaultQuietDuration is the quiet duration when --for is omitted.
const DefaultQuietDuration = 8 * time.Hour

// QuietHandler runs `whinny quiet`: it sets the quiet gate to
// now + --for (default 8h; --for 0 is indefinite). Records keep
// flowing; quiet only gates sends.
func QuietHandler(d Deps) clicontract.HandleFunc {
	return func(ctx context.Context, inv clicontract.Invocation, _ io.Writer) (any, error) {
		st, owned, err := d.storeOwned()
		if err != nil {
			return nil, err
		}
		if owned {
			defer st.Close()
		}
		forDur := DefaultQuietDuration
		if inv.Supplied["for"] {
			forDur = inv.Durations["for"]
		}
		if forDur < 0 {
			return nil, UsageErrorf("quiet: --for must be non-negative, got %s", forDur)
		}
		now := d.now()
		var until int64
		indefinite := false
		if forDur == 0 {
			until = QuietUntilIndefinite
			indefinite = true
		} else {
			until = now.Add(forDur).Unix()
		}
		if err := st.SetMeta(ctx, MetaQuietUntil, strconv.FormatInt(until, 10)); err != nil {
			return nil, OperationalErrorf("quiet: %v", err)
		}
		if indefinite {
			return "quiet: indefinite (until " + time.Unix(QuietUntilIndefinite, 0).UTC().Format("2006-01-02") + ")", nil
		}
		return "quiet until " + time.Unix(until, 0).UTC().Format(time.RFC3339), nil
	}
}

// ResumeHandler runs `whinny resume`: it clears the quiet gate.
func ResumeHandler(d Deps) clicontract.HandleFunc {
	return func(ctx context.Context, inv clicontract.Invocation, _ io.Writer) (any, error) {
		st, owned, err := d.storeOwned()
		if err != nil {
			return nil, err
		}
		if owned {
			defer st.Close()
		}
		if err := st.SetMeta(ctx, MetaQuietUntil, ""); err != nil {
			return nil, OperationalErrorf("resume: %v", err)
		}
		return "resumed: the quiet gate is cleared", nil
	}
}
