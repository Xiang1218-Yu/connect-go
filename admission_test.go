// Copyright 2021-2026 The Connect Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package connect_test

import (
	"context"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/internal/assert"
)

const (
	pingProcedure = "/connect.ping.v1.PingService/Ping"
	sumProcedure  = "/connect.ping.v1.PingService/Sum"
)

// fakeServerStream is a no-op [connect.ServerStream] for driving Server.Call
// directly.
type fakeServerStream struct{}

func (fakeServerStream) Receive(any) error  { return io.EOF }
func (fakeServerStream) SendHeaders() error { return nil }
func (fakeServerStream) Send(any) error     { return nil }

// blockingHandler blocks every call until release is closed and records the
// peak number of concurrently active calls.
type blockingHandler struct {
	entered atomic.Int64
	active  atomic.Int64
	peak    atomic.Int64
	release chan struct{}
}

func newBlockingHandler() *blockingHandler {
	return &blockingHandler{release: make(chan struct{})}
}

func (h *blockingHandler) handler(context.Context, connect.Spec, connect.ServerStream) error {
	h.entered.Add(1)
	active := h.active.Add(1)
	for {
		peak := h.peak.Load()
		if active <= peak || h.peak.CompareAndSwap(peak, active) {
			break
		}
	}
	<-h.release
	h.active.Add(-1)
	return nil
}

// callInGoroutine runs Server.Call in its own goroutine, returning a channel
// that yields the call's result.
func callInGoroutine(
	ctx context.Context,
	server *connect.Server,
	procedure string,
	stream connect.ServerStream,
) <-chan error {
	result := make(chan error, 1)
	go func() {
		result <- server.Call(ctx, procedure, nil, stream)
	}()
	return result
}

func waitUntil(t *testing.T, condition func() bool) {
	t.Helper()
	for range 10000 {
		if condition() {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("condition not met")
}

func TestServerAdmissionDisabledByDefault(t *testing.T) {
	t.Parallel()
	handler := newBlockingHandler()
	server := connect.NewServer()
	server.Register(connect.Method{
		Spec:    connect.Spec{Procedure: pingProcedure},
		Handler: handler.handler,
	})

	const calls = 32
	var wg sync.WaitGroup
	for range calls {
		wg.Go(func() {
			err := server.Call(t.Context(), pingProcedure, nil, fakeServerStream{})
			assert.Nil(t, err)
		})
	}
	waitUntil(t, func() bool { return handler.active.Load() == calls })
	close(handler.release)
	wg.Wait()
	assert.Equal(t, handler.peak.Load(), int64(calls))
}

func TestServerAdmissionServiceLimit(t *testing.T) {
	t.Parallel()
	handler := newBlockingHandler()
	server := connect.NewServer()
	server.SetConcurrencyLimits(connect.ConcurrencyLimits{ServiceLimit: 2})
	server.Register(connect.Method{
		Spec:    connect.Spec{Procedure: pingProcedure},
		Handler: handler.handler,
	})
	ctx := t.Context()

	first := callInGoroutine(ctx, server, pingProcedure, fakeServerStream{})
	second := callInGoroutine(ctx, server, pingProcedure, fakeServerStream{})
	waitUntil(t, func() bool { return handler.active.Load() == 2 })

	// The third call is rejected before the handler runs.
	err := server.Call(ctx, pingProcedure, nil, fakeServerStream{})
	assert.Equal(t, connect.CodeOf(err), connect.CodeResourceExhausted)
	assert.Match(t, err.Error(), "service concurrency limit")
	assert.Equal(t, handler.entered.Load(), int64(2))

	// Finishing one call frees a service-wide slot.
	close(handler.release)
	assert.Nil(t, <-first)
	assert.Nil(t, <-second)
	assert.Nil(t, server.Call(ctx, pingProcedure, nil, fakeServerStream{}))
	assert.Equal(t, handler.peak.Load(), int64(2))
}

func TestServerAdmissionMethodLimit(t *testing.T) {
	t.Parallel()
	pingHandler := newBlockingHandler()
	sumHandler := newBlockingHandler()
	server := connect.NewServer()
	server.SetConcurrencyLimits(connect.ConcurrencyLimits{
		MethodLimits: map[string]int{pingProcedure: 1},
	})
	server.Register(
		connect.Method{
			Spec:    connect.Spec{Procedure: pingProcedure},
			Handler: pingHandler.handler,
		},
		connect.Method{
			Spec:    connect.Spec{Procedure: sumProcedure},
			Handler: sumHandler.handler,
		},
	)
	ctx := t.Context()

	pingCall := callInGoroutine(ctx, server, pingProcedure, fakeServerStream{})
	waitUntil(t, func() bool { return pingHandler.active.Load() == 1 })

	// A second ping exceeds the independent method limit.
	err := server.Call(ctx, pingProcedure, nil, fakeServerStream{})
	assert.Equal(t, connect.CodeOf(err), connect.CodeResourceExhausted)
	assert.Match(t, err.Error(), `procedure ".*Ping"`)

	// The unlimited procedure is unaffected while ping is at capacity.
	sumCall := callInGoroutine(ctx, server, sumProcedure, fakeServerStream{})
	waitUntil(t, func() bool { return sumHandler.active.Load() == 1 })

	// Releasing the ping call restores its method slot.
	close(pingHandler.release)
	assert.Nil(t, <-pingCall)
	assert.Nil(t, server.Call(ctx, pingProcedure, nil, fakeServerStream{}))

	close(sumHandler.release)
	assert.Nil(t, <-sumCall)
}

func TestServerAdmissionMethodRejectionRollsBackServiceSlot(t *testing.T) {
	t.Parallel()
	pingHandler := newBlockingHandler()
	sumHandler := newBlockingHandler()
	server := connect.NewServer()
	server.SetConcurrencyLimits(connect.ConcurrencyLimits{
		ServiceLimit: 2,
		MethodLimits: map[string]int{
			pingProcedure: 1,
		},
	})
	server.Register(
		connect.Method{
			Spec:    connect.Spec{Procedure: pingProcedure},
			Handler: pingHandler.handler,
		},
		connect.Method{
			Spec:    connect.Spec{Procedure: sumProcedure},
			Handler: sumHandler.handler,
		},
	)
	ctx := t.Context()

	pingCall := callInGoroutine(ctx, server, pingProcedure, fakeServerStream{})
	waitUntil(t, func() bool { return pingHandler.active.Load() == 1 })

	// Service has a free slot, but the ping method limit is reached.
	err := server.Call(ctx, pingProcedure, nil, fakeServerStream{})
	assert.Equal(t, connect.CodeOf(err), connect.CodeResourceExhausted)
	assert.Match(t, err.Error(), `procedure ".*Ping"`)

	// The rolled-back service slot stays available: one sum call is admitted,
	// filling the service-wide limit of two alongside the held ping call.
	sumCall := callInGoroutine(ctx, server, sumProcedure, fakeServerStream{})
	waitUntil(t, func() bool { return sumHandler.active.Load() == 1 })

	// No service capacity remains, so a second sum call hits the service limit.
	err = server.Call(ctx, sumProcedure, nil, fakeServerStream{})
	assert.Equal(t, connect.CodeOf(err), connect.CodeResourceExhausted)
	assert.Match(t, err.Error(), "service concurrency limit")

	close(pingHandler.release)
	close(sumHandler.release)
	assert.Nil(t, <-pingCall)
	assert.Nil(t, <-sumCall)
}

func TestServerAdmissionServiceLimitCheckedBeforeMethodLimit(t *testing.T) {
	t.Parallel()
	handler := newBlockingHandler()
	server := connect.NewServer()
	server.SetConcurrencyLimits(connect.ConcurrencyLimits{
		ServiceLimit: 1,
		MethodLimits: map[string]int{
			pingProcedure: 1,
		},
	})
	server.Register(connect.Method{
		Spec:    connect.Spec{Procedure: pingProcedure},
		Handler: handler.handler,
	})
	ctx := t.Context()

	held := callInGoroutine(ctx, server, pingProcedure, fakeServerStream{})
	waitUntil(t, func() bool { return handler.active.Load() == 1 })

	// When both limits are saturated, the service-wide limit (checked first)
	// always explains the rejection.
	for range 50 {
		err := server.Call(ctx, pingProcedure, nil, fakeServerStream{})
		assert.Equal(t, connect.CodeOf(err), connect.CodeResourceExhausted)
		assert.Match(t, err.Error(), "service concurrency limit")
	}

	close(handler.release)
	assert.Nil(t, <-held)
}

func TestServerAdmissionReleasesAfterHandlerError(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	server := connect.NewServer()
	server.SetConcurrencyLimits(connect.ConcurrencyLimits{ServiceLimit: 1})
	handlerErr := connect.NewError(connect.CodeFailedPrecondition, "boom")
	server.Register(connect.Method{
		Spec: connect.Spec{Procedure: pingProcedure},
		Handler: func(context.Context, connect.Spec, connect.ServerStream) error {
			calls.Add(1)
			return handlerErr
		},
	})

	// Every call must run the handler: an exhausted slot would surface as
	// ResourceExhausted instead.
	for range 100 {
		err := server.Call(t.Context(), pingProcedure, nil, fakeServerStream{})
		assert.ErrorIs(t, err, handlerErr)
	}
	assert.Equal(t, calls.Load(), int64(100))
}

func TestServerAdmissionReleasesAfterPanic(t *testing.T) {
	t.Parallel()
	var panicCalls atomic.Int64
	blocking := newBlockingHandler()
	server := connect.NewServer()
	server.SetConcurrencyLimits(connect.ConcurrencyLimits{ServiceLimit: 1})
	server.Register(
		connect.Method{
			Spec: connect.Spec{Procedure: pingProcedure},
			Handler: func(context.Context, connect.Spec, connect.ServerStream) error {
				panicCalls.Add(1)
				panic("kaboom") //nolint:forbidigo // exercises lease release on panic
			},
		},
		connect.Method{
			Spec:    connect.Spec{Procedure: sumProcedure},
			Handler: blocking.handler,
		},
	)

	for range 25 {
		func() {
			defer func() {
				_ = recover()
			}()
			_ = server.Call(t.Context(), pingProcedure, nil, fakeServerStream{})
		}()
	}
	assert.Equal(t, panicCalls.Load(), int64(25))

	// The slot survived every panic: a blocking call on another procedure is
	// admitted and holds the only slot.
	held := callInGoroutine(t.Context(), server, sumProcedure, fakeServerStream{})
	waitUntil(t, func() bool { return blocking.active.Load() == 1 })
	err := server.Call(t.Context(), sumProcedure, nil, fakeServerStream{})
	assert.Equal(t, connect.CodeOf(err), connect.CodeResourceExhausted)
	close(blocking.release)
	assert.Nil(t, <-held)
}

func TestServerAdmissionReleasesOnContextCancel(t *testing.T) {
	t.Parallel()
	server := connect.NewServer()
	server.SetConcurrencyLimits(connect.ConcurrencyLimits{ServiceLimit: 1})
	server.Register(
		connect.Method{
			Spec: connect.Spec{Procedure: pingProcedure},
			Handler: func(ctx context.Context, _ connect.Spec, _ connect.ServerStream) error {
				<-ctx.Done()
				return ctx.Err()
			},
		},
		connect.Method{
			Spec: connect.Spec{Procedure: sumProcedure},
			Handler: func(context.Context, connect.Spec, connect.ServerStream) error {
				return nil
			},
		},
	)

	ctx, cancel := context.WithCancel(t.Context())
	canceled := callInGoroutine(ctx, server, pingProcedure, fakeServerStream{})
	cancel()
	assert.ErrorIs(t, <-canceled, context.Canceled)

	// Cancellation released the slot, so a fresh call on another procedure is
	// admitted immediately.
	err := server.Call(t.Context(), sumProcedure, nil, fakeServerStream{})
	assert.Nil(t, err)
}

func TestServerAdmissionNonPositiveLimitsIgnored(t *testing.T) {
	t.Parallel()
	handler := newBlockingHandler()
	server := connect.NewServer()
	server.SetConcurrencyLimits(connect.ConcurrencyLimits{
		ServiceLimit: 0,
		MethodLimits: map[string]int{
			pingProcedure: 0,
			sumProcedure:  -1,
		},
	})
	server.Register(connect.Method{
		Spec:    connect.Spec{Procedure: pingProcedure},
		Handler: handler.handler,
	})

	const calls = 8
	var wg sync.WaitGroup
	for range calls {
		wg.Go(func() {
			assert.Nil(t, server.Call(t.Context(), pingProcedure, nil, fakeServerStream{}))
		})
	}
	waitUntil(t, func() bool { return handler.active.Load() == calls })
	close(handler.release)
	wg.Wait()
}

func TestServerAdmissionZeroConfigRestoresDefaultBehavior(t *testing.T) {
	t.Parallel()
	handler := newBlockingHandler()
	server := connect.NewServer()
	server.SetConcurrencyLimits(connect.ConcurrencyLimits{ServiceLimit: 1})
	server.SetConcurrencyLimits(connect.ConcurrencyLimits{})
	server.Register(connect.Method{
		Spec:    connect.Spec{Procedure: pingProcedure},
		Handler: handler.handler,
	})

	const calls = 8
	var wg sync.WaitGroup
	for range calls {
		wg.Go(func() {
			assert.Nil(t, server.Call(t.Context(), pingProcedure, nil, fakeServerStream{}))
		})
	}
	waitUntil(t, func() bool { return handler.active.Load() == calls })
	close(handler.release)
	wg.Wait()
}

func TestServerAdmissionUnknownProcedureUsesServiceLimit(t *testing.T) {
	t.Parallel()
	handler := newBlockingHandler()
	server := connect.NewServer()
	server.SetConcurrencyLimits(connect.ConcurrencyLimits{ServiceLimit: 1})
	server.Register(connect.Method{
		Spec:    connect.Spec{Procedure: pingProcedure},
		Handler: handler.handler,
	})
	ctx := t.Context()

	held := callInGoroutine(ctx, server, pingProcedure, fakeServerStream{})
	waitUntil(t, func() bool { return handler.active.Load() == 1 })

	// An unregistered call still consumes service-wide capacity, so it is
	// rejected while the only slot is held.
	err := server.Call(ctx, "/connect.ping.v1.PingService/Missing", nil, fakeServerStream{})
	assert.Equal(t, connect.CodeOf(err), connect.CodeResourceExhausted)

	close(handler.release)
	assert.Nil(t, <-held)

	// With a free slot, the unknown procedure reaches the registry lookup.
	err = server.Call(ctx, "/connect.ping.v1.PingService/Missing", nil, fakeServerStream{})
	assert.Equal(t, connect.CodeOf(err), connect.CodeUnimplemented)
}

func TestServerAdmissionUnderConcurrentContention(t *testing.T) {
	t.Parallel()
	const (
		serviceLimit = 4
		methodLimit  = 2
		goroutines   = 64
	)
	var active atomic.Int64
	var peak atomic.Int64
	perProcedure := map[string]*atomic.Int64{
		pingProcedure: {},
		sumProcedure:  {},
	}
	perProcedurePeak := map[string]*atomic.Int64{
		pingProcedure: {},
		sumProcedure:  {},
	}
	server := connect.NewServer()
	server.SetConcurrencyLimits(connect.ConcurrencyLimits{
		ServiceLimit: serviceLimit,
		MethodLimits: map[string]int{
			pingProcedure: methodLimit,
		},
	})
	registerTracked := func(procedure string) {
		server.Register(connect.Method{
			Spec: connect.Spec{Procedure: procedure},
			Handler: func(context.Context, connect.Spec, connect.ServerStream) error {
				current := active.Add(1)
				methodActive := perProcedure[procedure].Add(1)
				for {
					observed := peak.Load()
					if current <= observed || peak.CompareAndSwap(observed, current) {
						break
					}
				}
				for {
					observed := perProcedurePeak[procedure].Load()
					if methodActive <= observed ||
						perProcedurePeak[procedure].CompareAndSwap(observed, methodActive) {
						break
					}
				}
				runtime.Gosched()
				active.Add(-1)
				perProcedure[procedure].Add(-1)
				return nil
			},
		})
	}
	registerTracked(pingProcedure)
	registerTracked(sumProcedure)

	var wg sync.WaitGroup
	var rejected atomic.Int64
	for i := range goroutines {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			procedure := pingProcedure
			if i%2 == 0 {
				procedure = sumProcedure
			}
			err := server.Call(t.Context(), procedure, nil, fakeServerStream{})
			if err != nil {
				assert.Equal(t, connect.CodeOf(err), connect.CodeResourceExhausted)
				rejected.Add(1)
			}
		}(i)
	}
	wg.Wait()

	assert.True(t, rejected.Load() > 0)
	assert.True(t, peak.Load() <= serviceLimit)
	assert.True(t, perProcedurePeak[pingProcedure].Load() <= methodLimit)
	// Every slot is released once the load drains.
	assert.Zero(t, active.Load())
	assert.Zero(t, perProcedure[pingProcedure].Load())
	assert.Zero(t, perProcedure[sumProcedure].Load())
	err := server.Call(t.Context(), pingProcedure, nil, fakeServerStream{})
	assert.Nil(t, err)
}
