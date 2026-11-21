package rate

import (
	"testing"
	"time"
)

var t0 = time.Unix(1700000000, 0)

func TestRefillStartsFull(t *testing.T) {
	st, ok := Refill(State{}, 2, t0)
	if !ok || st.Tokens != 2 || !st.Last.Equal(t0) {
		t.Fatalf("Refill(zero) = %+v, ok=%v; want full bucket at t0", st, ok)
	}
}

func TestRefillCapsAtCapacity(t *testing.T) {
	st := State{Tokens: 1, Last: t0}
	st, _ = Refill(st, 2, t0.Add(time.Hour))
	if st.Tokens != 2 {
		t.Fatalf("tokens after an hour = %v, want capped at 2", st.Tokens)
	}
}

func TestRefillClockBackwardKeepsLast(t *testing.T) {
	st := State{Tokens: 0.5, Last: t0}
	st, _ = Refill(st, 2, t0.Add(-time.Minute))
	if !st.Last.Equal(t0) {
		t.Fatalf("last moved to %v on a backward clock; want it pinned at t0", st.Last)
	}
	if st.Tokens != 0.5 {
		t.Fatalf("tokens = %v, want unchanged (no refund on a backward step)", st.Tokens)
	}
}

func TestFailClosed(t *testing.T) {
	if _, ok := Refill(State{}, 0, t0); ok {
		t.Fatal("perMinute 0 must fail closed")
	}
}

func TestAvailableAndTake(t *testing.T) {
	st := State{Tokens: 1, Last: t0}
	if ok, wait := Available(st, 2); !ok || wait != 0 {
		t.Fatalf("full bucket: ok=%v wait=%v, want true/0", ok, wait)
	}
	st = Take(st)
	ok, wait := Available(st, 2)
	if ok || wait != 30*time.Second {
		t.Fatalf("half-drained 2/min bucket: ok=%v wait=%v, want false/30s", ok, wait)
	}
	if empty, _ := Refill(st, 2, t0); true {
		if ok, _ := Available(empty, 2); ok {
			t.Fatal("empty bucket admitted")
		}
	}
}
