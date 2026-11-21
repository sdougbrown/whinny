package cli

import (
	"context"
	"io"

	"github.com/sdougbrown/cli-contract"
	"github.com/sdougbrown/whinny/internal/store"
)

// BreakerClearHandler runs `whinny breaker-clear`: it releases the
// send-path breaker latch and resets the consecutive post-failure
// counter, so the next post is allowed.
func BreakerClearHandler(d Deps) clicontract.HandleFunc {
	return func(ctx context.Context, inv clicontract.Invocation, _ io.Writer) (any, error) {
		st, owned, err := d.storeOwned()
		if err != nil {
			return nil, err
		}
		if owned {
			defer st.Close()
		}
		if err := st.SetBreaker(ctx, store.Breaker{}); err != nil {
			return nil, OperationalErrorf("breaker-clear: %v", err)
		}
		if err := st.ResetPostFailures(ctx); err != nil {
			return nil, OperationalErrorf("breaker-clear: %v", err)
		}
		return "breaker cleared: the latch is released and the failure counter reset", nil
	}
}
