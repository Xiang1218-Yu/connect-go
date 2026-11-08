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

package connecthttp_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"connectrpc.com/connect/v2/internal/assert"
	pingv1 "connectrpc.com/connect/v2/internal/gen/connect/ping/v1"
	"connectrpc.com/connect/v2/internal/gen/connect/ping/v1/pingv1connect"
	"connectrpc.com/connect/v2/internal/memhttp"
	"connectrpc.com/connect/v2/internal/memhttp/memhttptest"
)

type policyCtxKey struct{}

// newPolicyHTTPServer mounts the ping service behind a request counter.
func newPolicyHTTPServer(t *testing.T) (*memhttp.Server, *atomic.Int32) {
	t.Helper()
	var requests atomic.Int32
	mux := http.NewServeMux()
	srv := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(srv, pingServer{})
	connecthttp.Mount(mux, srv)
	server := memhttptest.NewServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		mux.ServeHTTP(w, r)
	}))
	return server, &requests
}

func newPolicyClient(t *testing.T, server *memhttp.Server, opts ...connecthttp.Option) pingv1connect.PingServiceClient {
	t.Helper()
	transport := connecthttp.NewTransport(server.Client(), server.URL(), opts...)
	return pingv1connect.NewPingServiceClient(connect.NewClient(transport))
}

// TestSendPolicyRejectsBeforeHTTPRequest verifies a rejecting policy blocks
// every RPC shape over every protocol before an HTTP request exists.
func TestSendPolicyRejectsBeforeHTTPRequest(t *testing.T) {
	t.Parallel()
	run := func(t *testing.T, opts ...connecthttp.Option) {
		t.Helper()
		server, requests := newPolicyHTTPServer(t)
		policyErr := connect.NewError(connect.CodePermissionDenied, "blocked by send policy")
		client := newPolicyClient(t, server, append(opts, connecthttp.WithSendPolicies(
			func(context.Context, connect.Spec, *connect.Header) error { return policyErr },
		))...)

		t.Run("unary", func(t *testing.T) {
			ctx, info := connect.NewClientContext(t.Context())
			_, err := client.Ping(ctx, &pingv1.PingRequest{Text: "hello"})
			assert.ErrorIs(t, err, policyErr)
			assert.Equal(t, connect.CodeOf(err), connect.CodePermissionDenied)
			assert.Equal(t, requests.Load(), int32(0))
			// The rejection must not pollute response metadata or stats.
			assert.Zero(t, info.ResponseHeader().Len())
			assert.Zero(t, info.ResponseTrailer().Len())
			assert.Zero(t, info.SendStats)
			assert.Zero(t, info.ReceiveStats)
			// No ClientInfo is attached before the policy chain allows the call.
			_, ok := connecthttp.ClientInfoForContext(ctx)
			assert.False(t, ok)
		})
		t.Run("client_stream", func(t *testing.T) {
			// The generated wrapper is a value type, so only the error and
			// absence of an HTTP request signal the rejection.
			_, err := client.Sum(t.Context())
			assert.ErrorIs(t, err, policyErr)
			assert.Equal(t, requests.Load(), int32(0))
		})
		t.Run("server_stream", func(t *testing.T) {
			_, err := client.CountUp(t.Context(), &pingv1.CountUpRequest{Number: 3})
			assert.ErrorIs(t, err, policyErr)
			assert.Equal(t, requests.Load(), int32(0))
		})
		t.Run("bidi_stream", func(t *testing.T) {
			_, err := client.CumSum(t.Context())
			assert.ErrorIs(t, err, policyErr)
			assert.Equal(t, requests.Load(), int32(0))
		})
	}
	t.Run("connect", func(t *testing.T) {
		t.Parallel()
		run(t)
	})
	t.Run("grpc", func(t *testing.T) {
		t.Parallel()
		run(t, connecthttp.WithGRPC())
	})
	t.Run("grpcweb", func(t *testing.T) {
		t.Parallel()
		run(t, connecthttp.WithGRPCWeb())
	})
}

// TestSendPolicyRejectsHTTPGet ensures an enabled Connect GET upgrade is
// also suppressed by a rejection.
func TestSendPolicyRejectsHTTPGet(t *testing.T) {
	t.Parallel()
	server, requests := newPolicyHTTPServer(t)
	policyErr := connect.NewError(connect.CodeFailedPrecondition, "no tenant")
	client := newPolicyClient(t, server,
		connecthttp.WithHTTPGet(),
		connecthttp.WithSendPolicies(func(context.Context, connect.Spec, *connect.Header) error {
			return policyErr
		}),
	)
	_, err := client.Ping(t.Context(), &pingv1.PingRequest{})
	assert.ErrorIs(t, err, policyErr)
	assert.Equal(t, requests.Load(), int32(0))
}

// TestSendPolicyReturnsLocalErrorsUnchanged verifies non-Connect errors pass
// through verbatim instead of being recoded or wrapped by the transport.
func TestSendPolicyReturnsLocalErrorsUnchanged(t *testing.T) {
	t.Parallel()
	server, requests := newPolicyHTTPServer(t)
	policyErr := errors.New("locally blocked")
	client := newPolicyClient(t, server, connecthttp.WithSendPolicies(
		func(context.Context, connect.Spec, *connect.Header) error { return policyErr },
	))
	_, err := client.Ping(t.Context(), &pingv1.PingRequest{})
	assert.ErrorIs(t, err, policyErr)
	assert.Equal(t, connect.CodeOf(err), connect.CodeUnknown)
	assert.Equal(t, requests.Load(), int32(0))
}

// TestSendPolicyChainOrderAndShortCircuit verifies policies run in
// configuration order, including across multiple WithSendPolicies options,
// and the first rejection stops the chain.
func TestSendPolicyChainOrderAndShortCircuit(t *testing.T) {
	t.Parallel()
	server, requests := newPolicyHTTPServer(t)
	var mu sync.Mutex
	var calls []string
	record := func(name string, reject bool) connect.SendPolicy {
		return func(context.Context, connect.Spec, *connect.Header) error {
			mu.Lock()
			calls = append(calls, name)
			mu.Unlock()
			if reject {
				return connect.Errorf(connect.CodeFailedPrecondition, "%s rejected", name)
			}
			return nil
		}
	}
	client := newPolicyClient(t, server,
		connecthttp.WithSendPolicies(record("one", false), record("two", true)),
		connecthttp.WithSendPolicies(record("three", false)),
	)
	_, err := client.Ping(t.Context(), &pingv1.PingRequest{})
	assert.Equal(t, connect.CodeOf(err), connect.CodeFailedPrecondition)
	assert.Equal(t, requests.Load(), int32(0))
	assert.Equal(t, calls, []string{"one", "two"})
}

// TestSendPolicyReceivesSpecAndRequestHeader verifies the policy observes the
// target procedure, stream type, idempotency level, plain and binary request
// headers, and the interceptor-derived context.
func TestSendPolicyReceivesSpecAndRequestHeader(t *testing.T) {
	t.Parallel()
	server, _ := newPolicyHTTPServer(t)
	var mu sync.Mutex
	var gotSpec connect.Spec
	var gotHeader connect.Header
	var gotCtxValue string
	policy := connect.SendPolicy(func(ctx context.Context, spec connect.Spec, header *connect.Header) error {
		mu.Lock()
		defer mu.Unlock()
		gotSpec = spec
		gotCtxValue, _ = ctx.Value(policyCtxKey{}).(string)
		for key, vals := range header.All() {
			gotHeader.SetValues(key, vals)
		}
		return nil
	})
	interceptor := func(next connect.ClientFunc) connect.ClientFunc {
		return func(ctx context.Context, spec connect.Spec) (connect.ClientStream, error) {
			info, _ := connect.CallInfoForClientContext(ctx)
			info.RequestHeader().Set("X-Interceptor", "seen")
			ctx = context.WithValue(ctx, policyCtxKey{}, "from-interceptor")
			return next(ctx, spec)
		}
	}
	transport := connecthttp.NewTransport(server.Client(), server.URL(), connecthttp.WithSendPolicies(policy))
	client := pingv1connect.NewPingServiceClient(connect.NewClient(transport, interceptor))

	ctx, info := connect.NewClientContext(t.Context())
	info.RequestHeader().Set("X-Tenant", "acme")
	const binaryValue = "binary-tenant"
	info.RequestHeader().Set("X-Tenant-Bin", connect.EncodeBinaryHeader([]byte(binaryValue)))
	if _, err := client.Ping(ctx, &pingv1.PingRequest{}); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	assert.Equal(t, gotSpec.Procedure, pingv1connect.PingServicePingProcedure)
	assert.Equal(t, gotSpec.StreamType, connect.StreamTypeUnary)
	assert.Equal(t, gotSpec.IdempotencyLevel, connect.IdempotencyNoSideEffects)
	assert.Equal(t, gotHeader.Get("X-Tenant"), "acme")
	assert.Equal(t, gotHeader.Get("X-Interceptor"), "seen")
	decoded, err := connect.DecodeBinaryHeader(gotHeader.Get("X-Tenant-Bin"))
	assert.Nil(t, err)
	assert.Equal(t, string(decoded), binaryValue)
	assert.Equal(t, gotCtxValue, "from-interceptor")

	// A streaming RPC reports its own stream type to the policy.
	bidiStream, err := client.CumSum(t.Context())
	if err != nil {
		t.Fatalf("CumSum open: %v", err)
	}
	assert.Nil(t, bidiStream.Close())
	mu.Lock()
	assert.Equal(t, gotSpec.StreamType, connect.StreamTypeBidi)
	mu.Unlock()
}

// TestSendPolicyOrderingWithInterceptors verifies the stable call order:
// client interceptors run first, then the policy chain, then the transport.
func TestSendPolicyOrderingWithInterceptors(t *testing.T) {
	t.Parallel()
	server, requests := newPolicyHTTPServer(t)
	var events []string
	var eventsMu sync.Mutex
	addEvent := func(event string) {
		eventsMu.Lock()
		events = append(events, event)
		eventsMu.Unlock()
	}
	policyErr := connect.NewError(connect.CodePermissionDenied, "policy denied")
	interceptor := func(next connect.ClientFunc) connect.ClientFunc {
		return func(ctx context.Context, spec connect.Spec) (connect.ClientStream, error) {
			addEvent("interceptor-enter")
			stream, err := next(ctx, spec)
			addEvent("interceptor-exit")
			return stream, err
		}
	}
	policy := connect.SendPolicy(func(context.Context, connect.Spec, *connect.Header) error {
		addEvent("policy")
		return policyErr
	})
	transport := connecthttp.NewTransport(server.Client(), server.URL(), connecthttp.WithSendPolicies(policy))
	client := pingv1connect.NewPingServiceClient(connect.NewClient(transport, interceptor))
	_, err := client.Ping(t.Context(), &pingv1.PingRequest{})
	assert.ErrorIs(t, err, policyErr)
	assert.Equal(t, requests.Load(), int32(0))
	assert.Equal(t, events, []string{"interceptor-enter", "policy", "interceptor-exit"})
}

// TestSendPolicyAllowsAllStreamTypes verifies an allowing chain leaves the
// full lifecycle, header propagation, and binary header encoding intact for
// the unary and all three streaming RPC shapes.
func TestSendPolicyAllowsAllStreamTypes(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	var mu sync.Mutex
	wireHeaders := http.Header{}
	mux := http.NewServeMux()
	srv := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(srv, pingServer{})
	connecthttp.Mount(mux, srv)
	server := memhttptest.NewServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		wireHeaders = r.Header.Clone()
		mu.Unlock()
		requests.Add(1)
		mux.ServeHTTP(w, r)
	}))
	client := newPolicyClient(t, server, connecthttp.WithSendPolicies(
		func(context.Context, connect.Spec, *connect.Header) error { return nil },
	))

	ctx, info := connect.NewClientContext(t.Context())
	info.RequestHeader().Set("X-Tenant", "acme")
	info.RequestHeader().Set("X-Tenant-Bin", connect.EncodeBinaryHeader([]byte("binary-tenant")))

	// Unary.
	if _, err := client.Ping(ctx, &pingv1.PingRequest{Number: 1, Text: "ping"}); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	// Client streaming.
	sumStream, err := client.Sum(ctx)
	if err != nil {
		t.Fatalf("Sum: %v", err)
	}
	assert.Nil(t, sumStream.Send(&pingv1.SumRequest{Number: 2}))
	assert.Nil(t, sumStream.Send(&pingv1.SumRequest{Number: 3}))
	sumResp, err := sumStream.CloseAndReceive()
	assert.Nil(t, err)
	assert.Equal(t, sumResp.GetSum(), int64(5))
	// Server streaming.
	countStream, err := client.CountUp(ctx, &pingv1.CountUpRequest{Number: 2})
	if err != nil {
		t.Fatalf("CountUp: %v", err)
	}
	var count int
	for {
		msg, err := countStream.Receive()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("CountUp Receive: %v", err)
		}
		count++
		_ = msg
	}
	assert.Nil(t, countStream.Close())
	assert.Equal(t, count, 2)
	// Bidi streaming.
	bidiStream, err := client.CumSum(ctx)
	if err != nil {
		t.Fatalf("CumSum: %v", err)
	}
	assert.Nil(t, bidiStream.Send(&pingv1.CumSumRequest{Number: 7}))
	if msg, err := bidiStream.Receive(); err != nil {
		t.Fatalf("CumSum Receive: %v", err)
	} else {
		assert.Equal(t, msg.GetSum(), int64(7))
	}
	assert.Nil(t, bidiStream.CloseSend())
	if _, err := bidiStream.Receive(); !errors.Is(err, io.EOF) {
		t.Fatalf("CumSum end = %v, want io.EOF", err)
	}
	assert.Nil(t, bidiStream.Close())

	assert.Equal(t, requests.Load(), int32(4))
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, wireHeaders.Get("X-Tenant"), "acme")
	decoded, err := connect.DecodeBinaryHeader(wireHeaders.Get("X-Tenant-Bin"))
	assert.Nil(t, err)
	assert.Equal(t, string(decoded), "binary-tenant")
}

// TestSendPolicyConditionalOptions verifies policies supplied through
// WithConditionalOptions apply per procedure without affecting other RPCs.
func TestSendPolicyConditionalOptions(t *testing.T) {
	t.Parallel()
	server, requests := newPolicyHTTPServer(t)
	policyErr := connect.NewError(connect.CodePermissionDenied, "ping blocked")
	client := newPolicyClient(t, server, connecthttp.WithConditionalOptions(
		func(spec connect.Spec) []connecthttp.Option {
			if spec.Procedure == pingv1connect.PingServicePingProcedure {
				return []connecthttp.Option{connecthttp.WithSendPolicies(
					func(context.Context, connect.Spec, *connect.Header) error { return policyErr },
				)}
			}
			return nil
		},
	))
	_, err := client.Ping(t.Context(), &pingv1.PingRequest{})
	assert.ErrorIs(t, err, policyErr)
	assert.Equal(t, requests.Load(), int32(0))

	stream, err := client.CountUp(t.Context(), &pingv1.CountUpRequest{Number: 1})
	if err != nil {
		t.Fatalf("CountUp: %v", err)
	}
	for {
		if _, err := stream.Receive(); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatalf("CountUp Receive: %v", err)
		}
	}
	assert.Nil(t, stream.Close())
	assert.Equal(t, requests.Load(), int32(1))
}
