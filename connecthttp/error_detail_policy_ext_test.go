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
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"connectrpc.com/connect/v2/connectproto"
	"connectrpc.com/connect/v2/internal/assert"
	"connectrpc.com/connect/v2/internal/memhttp"
	"connectrpc.com/connect/v2/internal/memhttp/memhttptest"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	detailUnaryProcedure  = "/connect.test.v1.DetailService/Unary"
	detailStreamProcedure = "/connect.test.v1.DetailService/ServerStream"
	detailRemoteProcedure = "/connect.test.v1.DetailService/Remote"
	detailCancelProcedure = "/connect.test.v1.DetailService/Cancel"

	detailTypeEmpty     = "google.protobuf.Empty"
	detailTypeDuration  = "google.protobuf.Duration"
	detailTypeTimestamp = "google.protobuf.Timestamp"
)

var errDetailSentinel = errors.New("local-only cause")

// detailPolicyServer serves custom procedures whose errors carry multiple
// detail types, including a duplicate, so policy filtering is observable.
type detailPolicyServer struct {
	stashed atomic.Pointer[connect.Error]
	richErr *connect.Error
}

func (s *detailPolicyServer) register(srv *connect.Server) {
	srv.Register(
		connect.Method{
			Spec:    connect.Spec{StreamType: connect.StreamTypeUnary, Procedure: detailUnaryProcedure},
			Handler: s.unary,
		},
		connect.Method{
			Spec:    connect.Spec{StreamType: connect.StreamTypeServer, Procedure: detailStreamProcedure},
			Handler: s.stream,
		},
		connect.Method{
			Spec:    connect.Spec{StreamType: connect.StreamTypeUnary, Procedure: detailRemoteProcedure},
			Handler: s.remote,
		},
		connect.Method{
			Spec:    connect.Spec{StreamType: connect.StreamTypeUnary, Procedure: detailCancelProcedure},
			Handler: s.cancel,
		},
	)
}

func newRichError(t *testing.T) *connect.Error {
	t.Helper()
	emptyFirst, err := connectproto.NewErrorDetail(&emptypb.Empty{})
	if err != nil {
		t.Fatalf("NewErrorDetail: %v", err)
	}
	duration, err := connectproto.NewErrorDetail(durationpb.New(7 * time.Second))
	if err != nil {
		t.Fatalf("NewErrorDetail: %v", err)
	}
	emptyDuplicate, err := connectproto.NewErrorDetail(&emptypb.Empty{})
	if err != nil {
		t.Fatalf("NewErrorDetail: %v", err)
	}
	timestamp, err := connectproto.NewErrorDetail(timestamppb.New(time.Unix(123, 456)))
	if err != nil {
		t.Fatalf("NewErrorDetail: %v", err)
	}
	return connect.NewError(connect.CodeFailedPrecondition, "policy public message").
		WithDetail(emptyFirst).
		WithDetail(duration).
		WithDetail(emptyDuplicate).
		WithDetail(timestamp)
}

func (s *detailPolicyServer) unary(context.Context, connect.Spec, connect.ServerStream) error {
	err := s.richErr.WithCause(errDetailSentinel)
	s.stashed.Store(err)
	return err
}

func (s *detailPolicyServer) stream(context.Context, connect.Spec, connect.ServerStream) error {
	return s.richErr
}

func (s *detailPolicyServer) remote(context.Context, connect.Spec, connect.ServerStream) error {
	// A peer's verdict, complete with internal details, must never be
	// forwarded as the handler's own error.
	return s.richErr.WithRemote()
}

func (s *detailPolicyServer) cancel(context.Context, connect.Spec, connect.ServerStream) error {
	return context.Canceled
}

func detailProtocolOptions() map[string][]connecthttp.Option {
	return map[string][]connecthttp.Option{
		"connect":  nil,
		"grpc":     {connecthttp.WithGRPC()},
		"grpc_web": {connecthttp.WithGRPCWeb()},
	}
}

func newDetailTestServer(t *testing.T, mountOpts ...connecthttp.Option) (*detailPolicyServer, *memhttp.Server) {
	t.Helper()
	srv := connect.NewServer()
	impl := &detailPolicyServer{richErr: newRichError(t)}
	impl.register(srv)
	mux := http.NewServeMux()
	connecthttp.Mount(mux, srv, mountOpts...)
	return impl, memhttptest.NewServer(t, mux)
}

func callDetailUnary(t *testing.T, client *connect.Client, procedure string) *connect.Error {
	t.Helper()
	spec := connect.Spec{StreamType: connect.StreamTypeUnary, Procedure: procedure}
	err := client.CallUnary(t.Context(), spec, &emptypb.Empty{}, &emptypb.Empty{})
	var connectErr *connect.Error
	if !assert.True(t, errors.As(err, &connectErr)) {
		t.Fatalf("CallUnary %s: %v", procedure, err)
	}
	return connectErr
}

func callDetailServerStream(t *testing.T, client *connect.Client, procedure string) *connect.Error {
	t.Helper()
	spec := connect.Spec{StreamType: connect.StreamTypeServer, Procedure: procedure}
	stream, err := client.CallServerStream(t.Context(), spec, &emptypb.Empty{})
	assert.Nil(t, err)
	for {
		if recvErr := stream.Receive(&emptypb.Empty{}); recvErr != nil {
			var connectErr *connect.Error
			if !assert.True(t, errors.As(recvErr, &connectErr)) {
				t.Fatalf("Receive: %v", recvErr)
			}
			return connectErr
		}
	}
}

func detailTypes(details []*connect.ErrorDetail) []string {
	types := make([]string, 0, len(details))
	for _, detail := range details {
		types = append(types, detail.Type)
	}
	return types
}

// TestErrorDetailPolicyFiltersAcrossProtocols verifies the same policy
// decision on every wire protocol: rejected types disappear, permitted
// details keep their order, duplicates, and binary bytes, and the code and
// message survive.
func TestErrorDetailPolicyFiltersAcrossProtocols(t *testing.T) {
	t.Parallel()
	// Permit Empty (zero-length payload, duplicated) and Duration (real
	// binary payload); Timestamp must be rejected.
	policy := connect.NewErrorDetailRules(
		connect.NewErrorDetailRule(detailTypeEmpty, detailTypeDuration),
	)
	impl, server := newDetailTestServer(t, connecthttp.WithErrorDetailPolicy(policy))

	wantTypes := []string{detailTypeEmpty, detailTypeDuration, detailTypeEmpty}
	wantDurationBytes := impl.richErr.Details()[1].Value
	if len(wantDurationBytes) == 0 {
		t.Fatal("test setup: duration detail must carry a non-empty payload")
	}

	for name, protocolOpts := range detailProtocolOptions() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			client := connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL(), protocolOpts...))

			connectErr := callDetailUnary(t, client, detailUnaryProcedure)
			assert.Equal(t, connectErr.Code(), connect.CodeFailedPrecondition)
			assert.Equal(t, connectErr.Message(), "policy public message")
			assert.Equal(t, detailTypes(connectErr.Details()), wantTypes)
			// Binary content is preserved byte-for-byte, and the duplicate
			// Empty details keep their positions around the Duration.
			assert.Equal(t, len(connectErr.Details()[0].Value), 0)
			assert.Equal(t, connectErr.Details()[1].Value, wantDurationBytes)
			assert.Equal(t, len(connectErr.Details()[2].Value), 0)
			assert.Equal(t, len(connectErr.Details()), 3)

			// Streaming end-of-stream errors pass through the same filter.
			streamErr := callDetailServerStream(t, client, detailStreamProcedure)
			assert.Equal(t, streamErr.Code(), connect.CodeFailedPrecondition)
			assert.Equal(t, detailTypes(streamErr.Details()), wantTypes)
			assert.Equal(t, streamErr.Details()[1].Value, wantDurationBytes)
		})
	}
}

// TestErrorDetailPolicyDisabledPreservesBehavior confirms that without the
// option, every locally authored detail still reaches every client.
func TestErrorDetailPolicyDisabledPreservesBehavior(t *testing.T) {
	t.Parallel()
	_, server := newDetailTestServer(t)
	wantTypes := []string{
		detailTypeEmpty,
		detailTypeDuration,
		detailTypeEmpty,
		detailTypeTimestamp,
	}
	for name, protocolOpts := range detailProtocolOptions() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			client := connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL(), protocolOpts...))
			connectErr := callDetailUnary(t, client, detailUnaryProcedure)
			assert.Equal(t, detailTypes(connectErr.Details()), wantTypes)
		})
	}
}

// TestErrorDetailPolicyProcedureOverride verifies method-level rules
// deterministically win over the service-level default.
func TestErrorDetailPolicyProcedureOverride(t *testing.T) {
	t.Parallel()
	policy := connect.NewErrorDetailRules(connect.NewErrorDetailRule(detailTypeTimestamp)).
		WithProcedureRule(detailUnaryProcedure, connect.NewErrorDetailRule(detailTypeEmpty))
	_, server := newDetailTestServer(t, connecthttp.WithErrorDetailPolicy(policy))

	for name, protocolOpts := range detailProtocolOptions() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			client := connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL(), protocolOpts...))

			// Override: only Empty details on the unary procedure.
			unaryErr := callDetailUnary(t, client, detailUnaryProcedure)
			assert.Equal(t, detailTypes(unaryErr.Details()), []string{detailTypeEmpty, detailTypeEmpty})

			// Default rule still applies to procedures without an override.
			streamErr := callDetailServerStream(t, client, detailStreamProcedure)
			assert.Equal(t, detailTypes(streamErr.Details()), []string{detailTypeTimestamp})
		})
	}
}

// TestErrorDetailPolicyConditionalOptions verifies per-procedure policies
// installed through WithConditionalOptions replace the service policy.
func TestErrorDetailPolicyConditionalOptions(t *testing.T) {
	t.Parallel()
	servicePolicy := connect.NewErrorDetailRules(connect.NewErrorDetailRule(detailTypeDuration))
	conditional := connecthttp.WithConditionalOptions(func(spec connect.Spec) []connecthttp.Option {
		if spec.Procedure != detailUnaryProcedure {
			return nil
		}
		return []connecthttp.Option{
			connecthttp.WithErrorDetailPolicy(
				connect.NewErrorDetailRules(connect.NewErrorDetailRule(detailTypeEmpty)),
			),
		}
	})
	_, server := newDetailTestServer(
		t,
		connecthttp.WithErrorDetailPolicy(servicePolicy),
		conditional,
	)

	client := connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL()))
	unaryErr := callDetailUnary(t, client, detailUnaryProcedure)
	assert.Equal(t, detailTypes(unaryErr.Details()), []string{detailTypeEmpty, detailTypeEmpty})

	streamErr := callDetailServerStream(t, client, detailStreamProcedure)
	assert.Equal(t, detailTypes(streamErr.Details()), []string{detailTypeDuration})
}

// TestErrorDetailPolicyKeepsOriginalErrorIntact confirms filtering never
// mutates the error the handler uses locally, and never serializes causes.
func TestErrorDetailPolicyKeepsOriginalErrorIntact(t *testing.T) {
	t.Parallel()
	policy := connect.NewErrorDetailRules(connect.NewErrorDetailRule(detailTypeEmpty))
	impl, server := newDetailTestServer(t, connecthttp.WithErrorDetailPolicy(policy))
	client := connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL()))

	connectErr := callDetailUnary(t, client, detailUnaryProcedure)
	assert.Equal(t, detailTypes(connectErr.Details()), []string{detailTypeEmpty, detailTypeEmpty})
	// The local cause cannot cross the network.
	assert.False(t, errors.Is(connectErr, errDetailSentinel))

	// The handler's original error is untouched: full detail list and cause.
	stashed := impl.stashed.Load()
	assert.NotNil(t, stashed)
	assert.Equal(t, len(stashed.Details()), 4)
	assert.True(t, errors.Is(stashed, errDetailSentinel))
}

// TestErrorDetailPolicyDoesNotLeakRemoteError confirms remote verdicts are
// scrubbed to CodeInternal with no message or details even when the policy
// would permit every type.
func TestErrorDetailPolicyDoesNotLeakRemoteError(t *testing.T) {
	t.Parallel()
	policy := connect.NewErrorDetailRules(connect.PermitAllErrorDetails())
	_, server := newDetailTestServer(t, connecthttp.WithErrorDetailPolicy(policy))

	for name, protocolOpts := range detailProtocolOptions() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			client := connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL(), protocolOpts...))
			connectErr := callDetailUnary(t, client, detailRemoteProcedure)
			assert.Equal(t, connectErr.Code(), connect.CodeInternal)
			assert.Equal(t, connectErr.Message(), "")
			assert.Zero(t, connectErr.Details())
			assert.True(t, connectErr.IsRemote())
		})
	}
}

// TestErrorDetailPolicyCancellationPath confirms cancellation cannot bypass
// the filter and does not gain details.
func TestErrorDetailPolicyCancellationPath(t *testing.T) {
	t.Parallel()
	policy := connect.NewErrorDetailRules(connect.NewErrorDetailRule()) // deny everything
	_, server := newDetailTestServer(t, connecthttp.WithErrorDetailPolicy(policy))

	for name, protocolOpts := range detailProtocolOptions() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			client := connect.NewClient(connecthttp.NewTransport(server.Client(), server.URL(), protocolOpts...))
			connectErr := callDetailUnary(t, client, detailCancelProcedure)
			assert.Equal(t, connectErr.Code(), connect.CodeCanceled)
			assert.Zero(t, connectErr.Details())
		})
	}
}

// TestErrorDetailPolicyErrorWriter verifies the standalone ErrorWriter
// applies the same policy as mounted handlers.
func TestErrorDetailPolicyErrorWriter(t *testing.T) {
	t.Parallel()
	policy := connect.NewErrorDetailRules(connect.NewErrorDetailRule(detailTypeEmpty))
	writer := connecthttp.NewErrorWriter(connecthttp.WithErrorDetailPolicy(policy))

	recorder := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, detailUnaryProcedure, nil)
	request.Header.Set("Content-Type", "application/json")
	assert.Nil(t, writer.Write(recorder, request, newRichError(t).WithCause(errDetailSentinel)))

	var wire struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Details []struct {
			Type string `json:"type"`
		} `json:"details"`
	}
	assert.Nil(t, json.Unmarshal(recorder.Body.Bytes(), &wire))
	assert.Equal(t, wire.Code, connect.CodeFailedPrecondition.String())
	assert.Equal(t, wire.Message, "policy public message")
	assert.Equal(t, len(wire.Details), 2)
	for _, detail := range wire.Details {
		assert.Equal(t, detail.Type, detailTypeEmpty)
	}
}
