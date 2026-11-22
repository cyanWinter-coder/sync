// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package singleflight

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type errValue struct{}

func (err *errValue) Error() string {
	return "error value"
}

func TestPanicErrorUnwrap(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name             string
		panicValue       any
		wrappedErrorType bool
	}{
		{
			name:             "panicError wraps non-error type",
			panicValue:       &panicError{value: "string value"},
			wrappedErrorType: false,
		},
		{
			name:             "panicError wraps error type",
			panicValue:       &panicError{value: new(errValue)},
			wrappedErrorType: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var recovered any

			group := &Group{}

			func() {
				defer func() {
					recovered = recover()
					t.Logf("after panic(%#v) in group.Do, recovered %#v", tc.panicValue, recovered)
				}()

				_, _, _ = group.Do(tc.name, func() (any, error) {
					panic(tc.panicValue)
				})
			}()

			if recovered == nil {
				t.Fatal("expected a non-nil panic value")
			}

			err, ok := recovered.(error)
			if !ok {
				t.Fatalf("recovered non-error type: %T", recovered)
			}

			if !errors.Is(err, new(errValue)) && tc.wrappedErrorType {
				t.Errorf("unexpected wrapped error type %T; want %T", err, new(errValue))
			}
		})
	}
}

func TestDo(t *testing.T) {
	var g Group
	v, err, _ := g.Do("key", func() (any, error) {
		return "bar", nil
	})
	if got, want := fmt.Sprintf("%v (%T)", v, v), "bar (string)"; got != want {
		t.Errorf("Do = %v; want %v", got, want)
	}
	if err != nil {
		t.Errorf("Do error = %v", err)
	}
}

func TestDoErr(t *testing.T) {
	var g Group
	someErr := errors.New("Some error")
	v, err, _ := g.Do("key", func() (any, error) {
		return nil, someErr
	})
	if err != someErr {
		t.Errorf("Do error = %v; want someErr %v", err, someErr)
	}
	if v != nil {
		t.Errorf("unexpected non-nil value %#v", v)
	}
}

func TestDoDupSuppress(t *testing.T) {
	var g Group
	var wg1, wg2 sync.WaitGroup
	c := make(chan string, 1)
	var calls int32
	fn := func() (any, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			// First invocation.
			wg1.Done()
		}
		v := <-c
		c <- v // pump; make available for any future calls

		time.Sleep(10 * time.Millisecond) // let more goroutines enter Do

		return v, nil
	}

	const n = 10
	wg1.Add(1)
	for i := 0; i < n; i++ {
		wg1.Add(1)
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			wg1.Done()
			v, err, _ := g.Do("key", fn)
			if err != nil {
				t.Errorf("Do error: %v", err)
				return
			}
			if s, _ := v.(string); s != "bar" {
				t.Errorf("Do = %T %v; want %q", v, v, "bar")
			}
		}()
	}
	wg1.Wait()
	// At least one goroutine is in fn now and all of them have at
	// least reached the line before the Do.
	c <- "bar"
	wg2.Wait()
	if got := atomic.LoadInt32(&calls); got <= 0 || got >= n {
		t.Errorf("number of calls = %d; want over 0 and less than %d", got, n)
	}
}

// Test that singleflight behaves correctly after Forget called.
// See https://github.com/golang/go/issues/31420
func TestForget(t *testing.T) {
	var g Group

	var (
		firstStarted  = make(chan struct{})
		unblockFirst  = make(chan struct{})
		firstFinished = make(chan struct{})
	)

	go func() {
		g.Do("key", func() (i any, e error) {
			close(firstStarted)
			<-unblockFirst
			close(firstFinished)
			return
		})
	}()
	<-firstStarted
	g.Forget("key")

	unblockSecond := make(chan struct{})
	secondResult := g.DoChan("key", func() (i any, e error) {
		<-unblockSecond
		return 2, nil
	})

	close(unblockFirst)
	<-firstFinished

	thirdResult := g.DoChan("key", func() (i any, e error) {
		return 3, nil
	})

	close(unblockSecond)
	<-secondResult
	r := <-thirdResult
	if r.Val != 2 {
		t.Errorf("We should receive result produced by second call, expected: 2, got %d", r.Val)
	}
}

func TestDoChan(t *testing.T) {
	var g Group
	ch := g.DoChan("key", func() (any, error) {
		return "bar", nil
	})

	res := <-ch
	v := res.Val
	err := res.Err
	if got, want := fmt.Sprintf("%v (%T)", v, v), "bar (string)"; got != want {
		t.Errorf("Do = %v; want %v", got, want)
	}
	if err != nil {
		t.Errorf("Do error = %v", err)
	}
}

// Test singleflight behaves correctly after Do panic.
// See https://github.com/golang/go/issues/41133
func TestPanicDo(t *testing.T) {
	var g Group
	fn := func() (any, error) {
		panic("invalid memory address or nil pointer dereference")
	}

	const n = 5
	waited := int32(n)
	panicCount := int32(0)
	done := make(chan struct{})
	for i := 0; i < n; i++ {
		go func() {
			defer func() {
				if err := recover(); err != nil {
					t.Logf("Got panic: %v\n%s", err, debug.Stack())
					atomic.AddInt32(&panicCount, 1)
				}

				if atomic.AddInt32(&waited, -1) == 0 {
					close(done)
				}
			}()

			g.Do("key", fn)
		}()
	}

	select {
	case <-done:
		if panicCount != n {
			t.Errorf("Expect %d panic, but got %d", n, panicCount)
		}
	case <-time.After(time.Second):
		t.Fatalf("Do hangs")
	}
}

func TestGoexitDo(t *testing.T) {
	var g Group
	fn := func() (any, error) {
		runtime.Goexit()
		return nil, nil
	}

	const n = 5
	waited := int32(n)
	done := make(chan struct{})
	for i := 0; i < n; i++ {
		go func() {
			var err error
			defer func() {
				if err != nil {
					t.Errorf("Error should be nil, but got: %v", err)
				}
				if atomic.AddInt32(&waited, -1) == 0 {
					close(done)
				}
			}()
			_, err, _ = g.Do("key", fn)
		}()
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("Do hangs")
	}
}

func TestPanicDoChan(t *testing.T) {
	var g Group
	ch := g.DoChan("key", func() (any, error) {
		panic("Panicking in DoChan")
	})

	select {
	case res := <-ch:
		if res.Err == nil {
			t.Fatalf("DoChan returned nil error after panic")
		}
		var p *panicError
		if !errors.As(res.Err, &p) {
			t.Fatalf("DoChan error = %T; want *panicError", res.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("DoChan did not deliver a Result after panic")
	}

	// A later call must not observe the panic as a live call.
	v, err, _ := g.Do("key", func() (any, error) { return "ok", nil })
	if err != nil || v != "ok" {
		t.Fatalf("Do after panic = (%v, %v); want (ok, nil)", v, err)
	}
}

func TestPanicDoSharedByDoChan(t *testing.T) {
	blocked := make(chan struct{})
	unblock := make(chan struct{})
	panicked := make(chan any, 1)

	var g Group
	go func() {
		defer func() {
			panicked <- recover()
		}()
		g.Do("key", func() (any, error) {
			close(blocked)
			<-unblock
			panic("Panicking in Do")
		})
	}()

	<-blocked
	ch := g.DoChan("key", func() (any, error) {
		panic("DoChan unexpectedly executed callback")
	})
	close(unblock)

	if r := <-panicked; r == nil {
		t.Fatal("Do did not propagate the panic to its caller")
	}

	select {
	case res := <-ch:
		if res.Err == nil {
			t.Fatalf("DoChan returned nil error for a panicking call")
		}
		if !strings.Contains(res.Err.Error(), "Panicking in Do") {
			t.Fatalf("DoChan error = %v; want it to describe the panic in Do", res.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("DoChan did not deliver a Result for a panicking call")
	}
}

func TestGoexitDoChan(t *testing.T) {
	var g Group
	ch := g.DoChan("key", func() (any, error) {
		runtime.Goexit()
		return nil, nil
	})

	select {
	case res := <-ch:
		if res.Err != errGoexit {
			t.Fatalf("DoChan error = %v; want %v", res.Err, errGoexit)
		}
	case <-time.After(time.Second):
		t.Fatal("DoChan did not deliver a Result after runtime.Goexit")
	}
}

// TestForgetLateCompletionDoesNotDeleteNewCall verifies that an older
// generation finishing after Forget cannot remove the newer call.
func TestForgetLateCompletionDoesNotDeleteNewCall(t *testing.T) {
	var g Group

	firstStarted := make(chan struct{})
	unblockFirst := make(chan struct{})
	firstResult := make(chan any, 1)

	go func() {
		v, _, _ := g.Do("key", func() (any, error) {
			close(firstStarted)
			<-unblockFirst
			return "first", nil
		})
		firstResult <- v
	}()
	<-firstStarted
	g.Forget("key")

	secondStarted := make(chan struct{})
	unblockSecond := make(chan struct{})
	secondResult := make(chan any, 1)
	go func() {
		v, _, _ := g.Do("key", func() (any, error) {
			close(secondStarted)
			<-unblockSecond
			return "second", nil
		})
		secondResult <- v
	}()
	<-secondStarted

	// The old generation completes first. Its conditional delete must
	// not remove the second, still in-flight call.
	close(unblockFirst)
	if v := <-firstResult; v != "first" {
		t.Fatalf("first caller got %v; want first", v)
	}

	thirdStarted := make(chan struct{})
	thirdResult := make(chan Result, 1)
	go func() {
		thirdResult <- <-g.DoChan("key", func() (any, error) {
			close(thirdStarted)
			return "third", nil
		})
	}()

	// Wait until the third caller has joined the second generation.
	waitForDups(t, &g, "key", 1)
	select {
	case <-thirdStarted:
		t.Fatal("third caller ran a new function; want it to share the second call")
	default:
	}

	close(unblockSecond)
	if v := <-secondResult; v != "second" {
		t.Fatalf("second caller got %v; want second", v)
	}
	res := <-thirdResult
	if res.Val != "second" || res.Err != nil {
		t.Fatalf("third caller got (%v, %v); want (second, nil)", res.Val, res.Err)
	}
	if !res.Shared {
		t.Error("third caller got Shared = false; want true")
	}
}

// TestForgetDoesNotCancelInFlightGeneration verifies that Forget leaves
// already-started work running for the callers that joined it.
func TestForgetDoesNotCancelInFlightGeneration(t *testing.T) {
	var g Group

	started := make(chan struct{})
	unblock := make(chan struct{})
	result := make(chan string, 1)

	go func() {
		v, err, _ := g.Do("key", func() (any, error) {
			close(started)
			<-unblock
			return "first", nil
		})
		if err != nil {
			t.Errorf("Do error: %v", err)
			result <- ""
			return
		}
		result <- v.(string)
	}()
	<-started

	// Forget while the first generation is still running, then start a
	// second generation. Both must be able to finish.
	g.Forget("key")

	secondDone := make(chan struct{})
	go func() {
		v, err, _ := g.Do("key", func() (any, error) { return "second", nil })
		if err != nil {
			t.Errorf("second Do error: %v", err)
		}
		if v != "second" {
			t.Errorf("second Do = %v; want second", v)
		}
		close(secondDone)
	}()
	close(unblock)

	select {
	case v := <-result:
		if v != "first" {
			t.Fatalf("first caller got %v; want first", v)
		}
	case <-time.After(time.Second):
		t.Fatal("Forget canceled or stranded an in-flight generation")
	}
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("the generation started after Forget did not complete")
	}
}

func TestDoContextCancelDoesNotAffectOthers(t *testing.T) {
	var g Group

	started := make(chan struct{})
	unblock := make(chan struct{})
	leader := make(chan Result, 1)
	go func() {
		v, err, _ := g.Do("key", func() (any, error) {
			close(started)
			<-unblock
			return "shared", nil
		})
		leader <- Result{Val: v, Err: err}
	}()
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	canceled := make(chan error, 1)
	go func() {
		_, err, _ := g.DoContext(ctx, "key", func() (any, error) {
			t.Error("DoContext executed its own function instead of joining")
			return nil, nil
		})
		canceled <- err
	}()

	// Wait for the DoContext caller to join, then cancel it. The leader's
	// work must be unaffected.
	waitForDups(t, &g, "key", 1)
	cancel()

	select {
	case err := <-canceled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("DoContext error = %v; want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("DoContext did not return after cancellation")
	}

	close(unblock)
	select {
	case res := <-leader:
		if res.Err != nil || res.Val != "shared" {
			t.Fatalf("leader = (%v, %v); want (shared, nil)", res.Val, res.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceling one waiter stopped the shared call")
	}
}

func TestDoChanContextCancelDeliversOnce(t *testing.T) {
	var g Group

	started := make(chan struct{})
	unblock := make(chan struct{})
	leader := g.DoChan("key", func() (any, error) {
		close(started)
		<-unblock
		return "shared", nil
	})
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	ch := g.DoChanContext(ctx, "key", func() (any, error) {
		t.Error("DoChanContext executed its own function instead of joining")
		return nil, nil
	})
	waitForDups(t, &g, "key", 1)
	cancel()

	select {
	case res := <-ch:
		if !errors.Is(res.Err, context.Canceled) {
			t.Fatalf("DoChanContext Err = %v; want context.Canceled", res.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("DoChanContext did not deliver a Result after cancellation")
	}

	// A cancellation must not be delivered to the channel a second time,
	// and the leader must still receive its result.
	close(unblock)
	select {
	case res := <-leader:
		if res.Val != "shared" || res.Err != nil {
			t.Fatalf("leader = (%v, %v); want (shared, nil)", res.Val, res.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceling one DoChan waiter stopped the shared call")
	}
	select {
	case res := <-ch:
		t.Fatalf("DoChanContext delivered a second Result: %+v", res)
	case <-time.After(20 * time.Millisecond):
	}
}

// waitForDups waits until the current generation of key has at least n
// duplicate callers.
func waitForDups(t *testing.T, g *Group, key string, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		g.mu.Lock()
		c := g.m[key]
		dups := -1
		if c != nil {
			dups = c.dups
		}
		g.mu.Unlock()
		if dups >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d duplicate callers; have %d", n, dups)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestDoChanEveryWaiterGetsOneResult(t *testing.T) {
	var g Group

	unblock := make(chan struct{})
	const n = 8
	var wg sync.WaitGroup
	var results int32
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch := g.DoChan("key", func() (any, error) {
				<-unblock
				return "v", nil
			})
			res := <-ch
			if res.Val != "v" || res.Err != nil {
				t.Errorf("DoChan = (%v, %v); want (v, nil)", res.Val, res.Err)
			}
			atomic.AddInt32(&results, 1)
		}()
	}

	waitForDups(t, &g, "key", n-1)
	close(unblock)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("some DoChan waiters never received a result")
	}
	if got := atomic.LoadInt32(&results); got != n {
		t.Fatalf("got %d results; want %d", got, n)
	}
}

func TestPanicEveryWaiterGetsOneResult(t *testing.T) {
	var g Group

	unblock := make(chan struct{})
	const n = 8
	var wg sync.WaitGroup
	delivered := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch := g.DoChan("key", func() (any, error) {
				<-unblock
				panic("boom")
			})
			delivered <- (<-ch).Err
		}()
	}

	waitForDups(t, &g, "key", n-1)
	close(unblock)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("some DoChan waiters were blocked by a panic")
	}
	close(delivered)
	for err := range delivered {
		var p *panicError
		if !errors.As(err, &p) {
			t.Fatalf("DoChan Err = %T (%v); want *panicError", err, err)
		}
	}
}

func TestGoexitEveryWaiterGetsOneResult(t *testing.T) {
	var g Group

	unblock := make(chan struct{})
	const n = 8
	var wg sync.WaitGroup
	delivered := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch := g.DoChan("key", func() (any, error) {
				<-unblock
				runtime.Goexit()
				return nil, nil
			})
			delivered <- (<-ch).Err
		}()
	}

	waitForDups(t, &g, "key", n-1)
	close(unblock)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("some DoChan waiters were blocked by runtime.Goexit")
	}
	close(delivered)
	for err := range delivered {
		if err != errGoexit {
			t.Fatalf("DoChan Err = %v; want %v", err, errGoexit)
		}
	}
}

// TestDoContextLeaderCancelDoesNotStopWork verifies that a context-aware
// caller that starts the call can abandon its wait without canceling the
// shared work for the callers that joined it.
func TestDoContextLeaderCancelDoesNotStopWork(t *testing.T) {
	var g Group

	started := make(chan struct{})
	unblock := make(chan struct{})
	var calls int32

	ctx, cancel := context.WithCancel(context.Background())
	leaderErr := make(chan error, 1)
	go func() {
		_, err, _ := g.DoContext(ctx, "key", func() (any, error) {
			atomic.AddInt32(&calls, 1)
			close(started)
			<-unblock
			return "shared", nil
		})
		leaderErr <- err
	}()
	<-started
	cancel()

	select {
	case err := <-leaderErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled leader returned %v; want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled leader did not return")
	}

	// A caller that joined the same generation still gets its result.
	follower := make(chan Result, 1)
	go func() {
		v, err, _ := g.Do("key", func() (any, error) {
			t.Error("follower executed its own function")
			return nil, nil
		})
		follower <- Result{Val: v, Err: err}
	}()
	waitForDups(t, &g, "key", 1)

	close(unblock)
	select {
	case res := <-follower:
		if res.Val != "shared" || res.Err != nil {
			t.Fatalf("follower = (%v, %v); want (shared, nil)", res.Val, res.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceling the starting caller stopped the shared work")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("fn ran %d times; want 1", got)
	}
}

// TestConcurrentForgetLifecycle hammers Forget against new generations
// and verifies that every caller still receives exactly one terminal
// outcome and no call leaks in the key table.
func TestConcurrentForgetLifecycle(t *testing.T) {
	var g Group

	stop := make(chan struct{})
	forgetDone := make(chan struct{})
	go func() {
		defer close(forgetDone)
		for {
			select {
			case <-stop:
				return
			default:
				g.Forget("key")
			}
		}
	}()

	const n = 32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			switch i % 3 {
			case 0:
				v, err, _ := g.Do("key", func() (any, error) { return i, nil })
				if err != nil || v == nil {
					t.Errorf("Do = (%v, %v); want a value and nil error", v, err)
				}
			case 1:
				res := <-g.DoChan("key", func() (any, error) { return i, nil })
				if res.Err != nil || res.Val == nil {
					t.Errorf("DoChan = (%v, %v); want a value and nil error", res.Val, res.Err)
				}
			case 2:
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_, err, _ := g.DoContext(ctx, "key", func() (any, error) { return i, nil })
				if !errors.Is(err, context.Canceled) {
					t.Errorf("DoContext with canceled ctx = %v; want context.Canceled", err)
				}
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("callers did not all terminate")
	}
	close(stop)
	<-forgetDone
}

func ExampleGroup() {
	g := new(Group)

	block := make(chan struct{})
	res1c := g.DoChan("key", func() (any, error) {
		<-block
		return "func 1", nil
	})
	res2c := g.DoChan("key", func() (any, error) {
		<-block
		return "func 2", nil
	})
	close(block)

	res1 := <-res1c
	res2 := <-res2c

	// Results are shared by functions executed with duplicate keys.
	fmt.Println("Shared:", res2.Shared)
	// Only the first function is executed: it is registered and started with "key",
	// and doesn't complete before the second function is registered with a duplicate key.
	fmt.Println("Equal results:", res1.Val.(string) == res2.Val.(string))
	fmt.Println("Result:", res1.Val)

	// Output:
	// Shared: true
	// Equal results: true
	// Result: func 1
}
