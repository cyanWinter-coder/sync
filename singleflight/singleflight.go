// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package singleflight provides a duplicate function call suppression
// mechanism.
package singleflight // import "golang.org/x/sync/singleflight"

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
)

// errGoexit indicates runtime.Goexit was called in
// the user-given function.
var errGoexit = errors.New("runtime.Goexit was called")

// A panicError is an arbitrary value recovered from a panic
// with the stack trace during the execution of the given function.
type panicError struct {
	value any
	stack []byte
}

// Error implements error interface.
func (p *panicError) Error() string {
	return fmt.Sprintf("%v\n\n%s", p.value, p.stack)
}

func (p *panicError) Unwrap() error {
	err, ok := p.value.(error)
	if !ok {
		return nil
	}

	return err
}

func newPanicError(v any) error {
	stack := debug.Stack()

	// The first line of the stack trace is of the form "goroutine N [status]:"
	// but by the time the panic reaches Do the goroutine may no longer exist
	// and its status will have changed. Trim out the misleading line.
	if line := bytes.IndexByte(stack[:], '\n'); line >= 0 {
		stack = stack[line+1:]
	}
	return &panicError{value: v, stack: stack}
}

// chanWaiter is one DoChan caller waiting for a call to terminate.
//
// delivered is guarded by Group.mu. A waiter is delivered to exactly
// once, either by the completing call or by its own canceled context,
// whichever acquires the mutex first.
type chanWaiter struct {
	ch        chan Result
	done      <-chan struct{} // the call's done channel
	delivered bool
}

// call is the in-flight call for one generation of a key.
//
// While a call is the current value of Group.m[key], new callers join
// it. Forget removes it from the key table without canceling it: the
// call keeps running for the callers that already joined it, but it can
// no longer accept new waiters.
type call struct {
	// done is closed exactly once, after the terminal state (val, err,
	// shared) has been published and every channel waiter has been
	// notified. Waiters read the remaining fields only after <-done.
	done chan struct{}

	val    any
	err    error
	shared bool

	// These fields are read and written with the singleflight mutex
	// held. dups counts callers that joined an existing call; chans
	// holds this generation's DoChan waiters.
	dups  int
	chans []*chanWaiter
}

// Group represents a class of work and forms a namespace in
// which units of work can be executed with duplicate suppression.
type Group struct {
	mu sync.Mutex       // protects m
	m  map[string]*call // lazily initialized
}

// Result holds the results of Do, so they can be passed
// on a channel.
type Result struct {
	Val    any
	Err    error
	Shared bool
}

// Do executes and returns the results of the given function, making
// sure that only one execution is in-flight for a given key at a
// time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
// The return value shared indicates whether v was given to multiple callers.
//
// A panic or runtime.Goexit in fn is propagated to every Do caller:
// each caller observes the same panic value or exits its own goroutine.
func (g *Group) Do(key string, fn func() (any, error)) (v any, err error, shared bool) {
	return g.do(context.Background(), key, fn, true)
}

// DoContext is like Do, but the caller's wait is bounded by ctx.
//
// Canceling ctx abandons only this caller's wait. Work that has already
// started for key continues for the remaining waiters, and a caller
// that joined an earlier generation is unaffected by Forget. If ctx is
// canceled before the shared call terminates, DoContext returns
// ctx.Err().
//
// Unlike Do, DoContext never executes fn on the calling goroutine, so
// the caller's cancellation cannot delay or cancel the shared work.
func (g *Group) DoContext(ctx context.Context, key string, fn func() (any, error)) (v any, err error, shared bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err, false
	}
	return g.do(ctx, key, fn, false)
}

func (g *Group) do(ctx context.Context, key string, fn func() (any, error), inline bool) (any, error, bool) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		c.dups++
		g.mu.Unlock()
		return g.wait(ctx, c)
	}
	c := &call{done: make(chan struct{})}
	g.m[key] = c
	g.mu.Unlock()

	if inline {
		// Preserve the historical Do behavior: the leader executes fn
		// on its own goroutine, so a panic or runtime.Goexit unwinds
		// through the caller's stack.
		g.doCall(c, key, fn)
		g.propagate(c)
		return c.val, c.err, c.shared
	}

	// A context-aware caller must not own the work: run fn in its own
	// goroutine so that canceling the caller leaves the call running
	// for everybody else.
	go g.doCall(c, key, fn)
	return g.wait(ctx, c)
}

// wait waits for c to terminate or for ctx to be canceled.
func (g *Group) wait(ctx context.Context, c *call) (any, error, bool) {
	if ctx.Done() != nil {
		select {
		case <-c.done:
			// Prefer the terminal result over a cancellation that
			// became ready at the same time.
		case <-ctx.Done():
			select {
			case <-c.done:
			default:
				return nil, ctx.Err(), false
			}
		}
	} else {
		<-c.done
	}
	g.propagate(c)
	return c.val, c.err, c.shared
}

// propagate replays a panic or runtime.Goexit on the calling goroutine.
//
// It is used only by Do-style callers. DoChan callers instead receive
// the panic or Goexit as a Result on their channel, so an unrecoverable
// panic cannot strand them.
func (g *Group) propagate(c *call) {
	if e, ok := c.err.(*panicError); ok {
		panic(e)
	}
	if c.err == errGoexit {
		runtime.Goexit()
	}
}

// DoChan is like Do but returns a channel that will receive the
// results when they are ready.
//
// The returned channel will not be closed. Every call to DoChan that
// shares a call receives exactly one Result, including when the shared
// function panics or calls runtime.Goexit: in those cases Result.Err
// carries the panic value or errGoexit.
func (g *Group) DoChan(key string, fn func() (any, error)) <-chan Result {
	return g.doChan(context.Background(), key, fn)
}

// DoChanContext is like DoChan, but the caller's wait is bounded by ctx.
//
// Canceling ctx abandons only this caller's wait; it does not cancel
// the shared function or the waits of the other callers. If ctx is
// canceled first, the returned channel receives a Result whose Err is
// ctx.Err(). The channel receives exactly one Result either way.
func (g *Group) DoChanContext(ctx context.Context, key string, fn func() (any, error)) <-chan Result {
	return g.doChan(ctx, key, fn)
}

func (g *Group) doChan(ctx context.Context, key string, fn func() (any, error)) <-chan Result {
	ch := make(chan Result, 1)
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		ch <- Result{Err: err}
		return ch
	}

	w := &chanWaiter{ch: ch}

	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		c.dups++
		w.done = c.done
		c.chans = append(c.chans, w)
		g.mu.Unlock()
	} else {
		c := &call{done: make(chan struct{})}
		w.done = c.done
		c.chans = append(c.chans, w)
		g.m[key] = c
		g.mu.Unlock()
		go g.doCall(c, key, fn)
	}

	if ctx.Done() != nil {
		go g.watchWaiter(ctx, w)
	}
	return ch
}

// watchWaiter delivers ctx's cancellation to w unless the call has
// already delivered its terminal Result.
func (g *Group) watchWaiter(ctx context.Context, w *chanWaiter) {
	select {
	case <-w.done:
		return
	case <-ctx.Done():
	}

	g.mu.Lock()
	if !w.delivered {
		w.delivered = true
		w.ch <- Result{Err: ctx.Err()}
	}
	g.mu.Unlock()
}

// doCall handles the single call for a key and completes it exactly once.
func (g *Group) doCall(c *call, key string, fn func() (any, error)) {
	normalReturn := false
	recovered := false

	// use double-defer to distinguish panic from runtime.Goexit,
	// more details see https://golang.org/cl/134395
	defer func() {
		// The given function invoked runtime.Goexit.
		if !normalReturn && !recovered {
			c.err = errGoexit
		}

		g.mu.Lock()
		c.shared = c.dups > 0

		// Remove only this generation from the key table. A newer call
		// may have been installed after Forget; a late completion of an
		// older generation must not delete it.
		if g.m[key] == c {
			delete(g.m, key)
		}

		// Deliver the terminal state to every channel waiter exactly
		// once, then wake the Do-style waiters. Both happen under the
		// group mutex, so a canceled waiter and a completing call
		// cannot both deliver to the same channel.
		for _, w := range c.chans {
			if w.delivered {
				continue
			}
			w.delivered = true
			w.ch <- Result{Val: c.val, Err: c.err, Shared: c.shared}
		}
		close(c.done)
		g.mu.Unlock()
	}()

	func() {
		defer func() {
			if !normalReturn {
				// Ideally, we would wait to take a stack trace until we've determined
				// whether this is a panic or a runtime.Goexit.
				//
				// Unfortunately, the only way we can distinguish the two is to see
				// whether the recover stopped the goroutine from terminating, and by
				// the time we know that, the part of the stack trace relevant to the
				// panic has been discarded.
				if r := recover(); r != nil {
					c.err = newPanicError(r)
				}
			}
		}()

		c.val, c.err = fn()
		normalReturn = true
	}()

	if !normalReturn {
		recovered = true
	}
}

// Forget tells the singleflight to forget about a key. Future calls
// to Do for this key will call the function rather than waiting for
// an earlier call to complete.
//
// Forget starts a new generation for key. It does not cancel work that
// has already started: existing waiters still receive the earlier
// call's result, and when that call finishes it removes only its own
// entry from the key table.
func (g *Group) Forget(key string) {
	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
}
