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

package connectinprocess_test

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectinprocess"
	"connectrpc.com/connect/v2/internal/assert"
	pingv1 "connectrpc.com/connect/v2/internal/gen/connect/ping/v1"
	"connectrpc.com/connect/v2/internal/gen/connect/ping/v1/pingv1connect"
)

type inProcessPolicyCtxKey struct{}

// countingPingHandler wraps pingServer and records every handler entry,
// proving a rejected RPC never starts the server-side Handler.
type countingPingHandler struct {
	pingServer

	calls atomic.Int32

	tenantMu sync.Mutex
	tenant   string
}

func (s *countingPingHandler) note(ctx context.Context) {
	s.calls.Add(1)
	if info, ok := connect.CallInfoForServerContext(ctx); ok {
		s.tenantMu.Lock()
		s.tenant = info.RequestHeader().Get("X-Tenant")
		s.tenantMu.Unlock()
	}
}

func (s *countingPingHandler) Ping(ctx context.Context, req *pingv1.PingRequest) (*pingv1.PingResponse, error) {
	s.note(ctx)
	return s.pingServer.Ping(ctx, req)
}

func (s *countingPingHandler) Sum(ctx context.Context, stream pingv1connect.PingServiceSumServerStream) (*pingv1.SumResponse, error) {
	s.note(ctx)
	return s.pingServer.Sum(ctx, stream)
}

func (s *countingPingHandler) CountUp(ctx context.Context, req *pingv1.CountUpRequest, stream pingv1connect.PingServiceCountUpServerStream) error {
	s.note(ctx)
	return s.pingServer.CountUp(ctx, req, stream)
}

func (s *countingPingHandler) CumSum(ctx context.Context, stream pingv1connect.PingServiceCumSumServerStream) error {
	s.note(ctx)
	return s.pingServer.CumSum(ctx, stream)
}

func (s *countingPingHandler) tenantValue() string {
	s.tenantMu.Lock()
	defer s.tenantMu.Unlock()
	return s.tenant
}

func newInProcessPolicyClient(
	t *testing.T,
	handler pingv1connect.PingServiceHandler,
	opts ...connectinprocess.Option,
) pingv1connect.PingServiceClient {
	t.Helper()
	server := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(server, handler)
	return pingv1connect.NewPingServiceClient(connect.NewClient(connectinprocess.New(server, opts...)))
}

// TestInProcessSendPolicyRejectsBeforeHandler verifies a rejecting policy
// blocks every RPC shape before the in-process handler starts, returning the
// local error without polluting response metadata or stats.
func TestInProcessSendPolicyRejectsBeforeHandler(t *testing.T) {
	t.Parallel()
	run := func(t *testing.T) {
		t.Helper()
		handler := &countingPingHandler{}
		policyErr := connect.NewError(connect.CodePermissionDenied, "blocked by send policy")
		client := newInProcessPolicyClient(t, handler, connectinprocess.WithSendPolicies(
			func(context.Context, connect.Spec, *connect.Header) error { return policyErr },
		))

		t.Run("unary", func(t *testing.T) {
			t.Parallel()
			ctx, info := connect.NewClientContext(t.Context())
			_, err := client.Ping(ctx, &pingv1.PingRequest{Text: "hello"})
			assert.ErrorIs(t, err, policyErr)
			assert.Equal(t, connect.CodeOf(err), connect.CodePermissionDenied)
			assert.Equal(t, handler.calls.Load(), int32(0))
			assert.Zero(t, info.ResponseHeader().Len())
			assert.Zero(t, info.ResponseTrailer().Len())
			assert.Zero(t, info.SendStats)
			assert.Zero(t, info.ReceiveStats)
		})
		t.Run("client_stream", func(t *testing.T) {
			t.Parallel()
			_, err := client.Sum(t.Context())
			assert.ErrorIs(t, err, policyErr)
			assert.Equal(t, handler.calls.Load(), int32(0))
		})
		t.Run("server_stream", func(t *testing.T) {
			t.Parallel()
			_, err := client.CountUp(t.Context(), &pingv1.CountUpRequest{Number: 3})
			assert.ErrorIs(t, err, policyErr)
			assert.Equal(t, handler.calls.Load(), int32(0))
		})
		t.Run("bidi_stream", func(t *testing.T) {
			t.Parallel()
			_, err := client.CumSum(t.Context())
			assert.ErrorIs(t, err, policyErr)
			assert.Equal(t, handler.calls.Load(), int32(0))
		})
	}
	run(t)
}

// TestInProcessSendPolicyChainShortCircuit verifies in-process policies run
// in order and the first rejection stops the chain and the handler.
func TestInProcessSendPolicyChainShortCircuit(t *testing.T) {
	t.Parallel()
	handler := &countingPingHandler{}
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
	client := newInProcessPolicyClient(t, handler,
		connectinprocess.WithSendPolicies(record("one", false), record("two", true)),
		connectinprocess.WithSendPolicies(record("three", false)),
	)
	_, err := client.Ping(t.Context(), &pingv1.PingRequest{})
	assert.Equal(t, connect.CodeOf(err), connect.CodeFailedPrecondition)
	assert.Equal(t, handler.calls.Load(), int32(0))
	assert.Equal(t, calls, []string{"one", "two"})
}

// TestInProcessSendPolicyInputs verifies the policy observes the procedure,
// stream type, idempotency level, plain and binary request headers, and the
// interceptor-derived context.
func TestInProcessSendPolicyInputs(t *testing.T) {
	t.Parallel()
	handler := &countingPingHandler{}
	var mu sync.Mutex
	var gotSpec connect.Spec
	var gotHeader connect.Header
	var gotCtxValue string
	policy := connect.SendPolicy(func(ctx context.Context, spec connect.Spec, header *connect.Header) error {
		mu.Lock()
		defer mu.Unlock()
		gotSpec = spec
		gotCtxValue, _ = ctx.Value(inProcessPolicyCtxKey{}).(string)
		for key, vals := range header.All() {
			gotHeader.SetValues(key, vals)
		}
		return nil
	})
	interceptor := func(next connect.ClientFunc) connect.ClientFunc {
		return func(ctx context.Context, spec connect.Spec) (connect.ClientStream, error) {
			info, _ := connect.CallInfoForClientContext(ctx)
			info.RequestHeader().Set("X-Interceptor", "seen")
			ctx = context.WithValue(ctx, inProcessPolicyCtxKey{}, "from-interceptor")
			return next(ctx, spec)
		}
	}
	server := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(server, handler)
	transport := connectinprocess.New(server, connectinprocess.WithSendPolicies(policy))
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

	// Opening a bidi stream passes the policy (its handler only dispatches on
	// the first Send or Receive), then abandon it without starting the handler.
	bidi, err := client.CumSum(t.Context())
	if err != nil {
		t.Fatalf("CumSum open: %v", err)
	}
	assert.Nil(t, bidi.Close())
	mu.Lock()
	assert.Equal(t, gotSpec.StreamType, connect.StreamTypeBidi)
	mu.Unlock()
	assert.Equal(t, handler.calls.Load(), int32(1))
}

// TestInProcessSendPolicyAllows verifies an allowing chain preserves the
// existing in-process lifecycle, including header reuse across calls, for
// unary and streaming RPCs.
func TestInProcessSendPolicyAllows(t *testing.T) {
	t.Parallel()
	handler := &countingPingHandler{}
	client := newInProcessPolicyClient(t, handler, connectinprocess.WithSendPolicies(
		func(context.Context, connect.Spec, *connect.Header) error { return nil },
		func(context.Context, connect.Spec, *connect.Header) error { return nil },
	))

	ctx, info := connect.NewClientContext(t.Context())
	info.RequestHeader().Set("X-Tenant", "acme")

	resp, err := client.Ping(ctx, &pingv1.PingRequest{Number: 9, Text: "hello"})
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	assert.Equal(t, resp.GetNumber(), int64(9))
	assert.Equal(t, resp.GetText(), "hello")
	assert.Equal(t, handler.tenantValue(), "acme")

	// Client streaming still runs the full exchange.
	stream, err := client.Sum(ctx)
	if err != nil {
		t.Fatalf("Sum: %v", err)
	}
	assert.Nil(t, stream.Send(&pingv1.SumRequest{Number: 4}))
	assert.Nil(t, stream.Send(&pingv1.SumRequest{Number: 5}))
	sumResp, err := stream.CloseAndReceive()
	assert.Nil(t, err)
	assert.Equal(t, sumResp.GetSum(), int64(9))

	// Server streaming reaches EOF cleanly.
	countStream, err := client.CountUp(ctx, &pingv1.CountUpRequest{Number: 2})
	if err != nil {
		t.Fatalf("CountUp: %v", err)
	}
	var count int
	for {
		if _, err := countStream.Receive(); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatalf("CountUp Receive: %v", err)
		}
		count++
	}
	assert.Nil(t, countStream.Close())
	assert.Equal(t, count, 2)
	assert.Equal(t, handler.calls.Load(), int32(3))
}

// TestInProcessWithoutSendPolicyUnchanged verifies the no-policy path keeps
// the original behavior.
func TestInProcessWithoutSendPolicyUnchanged(t *testing.T) {
	t.Parallel()
	handler := &countingPingHandler{}
	server := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(server, handler)
	client := pingv1connect.NewPingServiceClient(connect.NewClient(connectinprocess.New(server)))
	resp, err := client.Ping(t.Context(), &pingv1.PingRequest{Number: 1})
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	assert.Equal(t, resp.GetNumber(), int64(1))
	assert.Equal(t, handler.calls.Load(), int32(1))
}
