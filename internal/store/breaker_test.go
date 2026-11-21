package store

import (
	"sync"
	"testing"
)

// TestBumpPostFailures: sequential bumps count up one at a time, the
// reset returns the count to 0, and a bump after the reset starts
// again at 1.
func TestBumpPostFailures(t *testing.T) {
	s := openTestStore(t)
	for i := 1; i <= 5; i++ {
		n, err := s.BumpPostFailures(testCtx)
		if err != nil {
			t.Fatalf("BumpPostFailures %d: %v", i, err)
		}
		if n != i {
			t.Fatalf("BumpPostFailures %d = %d, want %d", i, n, i)
		}
	}
	if err := s.ResetPostFailures(testCtx); err != nil {
		t.Fatalf("ResetPostFailures: %v", err)
	}
	v, err := s.GetMeta(testCtx, MetaPostFailures)
	if err != nil {
		t.Fatalf("GetMeta: %v", err)
	}
	if v != "0" {
		t.Fatalf("post_failures = %q, want 0 after the reset", v)
	}
	if n, err := s.BumpPostFailures(testCtx); err != nil || n != 1 {
		t.Fatalf("BumpPostFailures after reset = (%d, %v), want (1, nil)", n, err)
	}
}

// TestBumpPostFailuresConcurrent: concurrent bumpers under the store's
// write mutex never lose an increment — N goroutines of M bumps each
// land at N*M.
func TestBumpPostFailuresConcurrent(t *testing.T) {
	s := openTestStore(t)
	const goroutines, bumps = 10, 25
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < bumps; j++ {
				if _, err := s.BumpPostFailures(testCtx); err != nil {
					t.Errorf("BumpPostFailures: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	n, err := s.BumpPostFailures(testCtx)
	if err != nil {
		t.Fatalf("final BumpPostFailures: %v", err)
	}
	if n != goroutines*bumps+1 {
		t.Fatalf("count = %d, want %d (no lost increments)", n, goroutines*bumps+1)
	}
}
