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
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectinprocess"
	"connectrpc.com/connect/v2/connectproto"
	"connectrpc.com/connect/v2/internal/assert"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	inProcessUnaryProcedure  = "/connect.test.v1.DetailService/Unary"
	inProcessStreamProcedure = "/connect.test.v1.DetailService/ServerStream"
	inProcessRemoteProcedure = "/connect.test.v1.DetailService/Remote"
	inProcessPanicProcedure  = "/connect.test.v1.DetailService/Panic"
	inProcessCancelProcedure = "/connect.test.v1.DetailService/Cancel"

	inProcessTypeEmpty     = "google.protobuf.Empty"
	inProcessTypeDuration  = "google.protobuf.Duration"
	inProcessTypeTimestamp = "google.protobuf.Timestamp"
)

var errInProcessSentinel = errors.New("local-only cause")

type inProcessDetailServer struct {
	stashed atomic.Pointer[connect.Error]
	richErr *connect.Error
}

func (s *inProcessDetailServer) register(srv *connect.Server) {
	srv.Register(
		connect.Method{
			Spec:    connect.Spec{StreamType: connect.StreamTypeUnary, Procedure: inProcessUnaryProcedure},
			Handler: s.unary,
		},
		connect.Method{
			Spec:    connect.Spec{StreamType: connect.StreamTypeServer, Procedure: inProcessStreamProcedure},
			Handler: s.stream,
		},
		connect.Method{
			Spec:    connect.Spec{StreamType: connect.StreamTypeUnary, Procedure: inProcessRemoteProcedure},
			Handler: s.remote,
		},
		connect.Method{
			Spec:    connect.Spec{StreamType: connect.StreamTypeUnary, Procedure: inProcessPanicProcedure},
			Handler: s.panic,
		},
		connect.Method{
			Spec:    connect.Spec{StreamType: connect.StreamTypeUnary, Procedure: inProcessCancelProcedure},
			Handler: s.cancel,
		},
	)
}

func newInProcessRichError(t *testing.T) *connect.Error {
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

func (s *inProcessDetailServer) unary(context.Context, connect.Spec, connect.ServerStream) error {
	err := s.richErr.WithCause(errInProcessSentinel)
	s.stashed.Store(err)
	return err
}

func (s *inProcessDetailServer) stream(context.Context, connect.Spec, connect.ServerStream) error {
	return s.richErr
}

func (s *inProcessDetailServer) remote(context.Context, connect.Spec, connect.ServerStream) error {
	return s.richErr.WithRemote()
}

func (s *inProcessDetailServer) panic(context.Context, connect.Spec, connect.ServerStream) error {
	panic("boom") //nolint:forbidigo // exercises panic recovery under a policy
}

func (s *inProcessDetailServer) cancel(context.Context, connect.Spec, connect.ServerStream) error {
	return context.Canceled
}

func newInProcessClient(t *testing.T, policy connect.ErrorDetailPolicy) (*inProcessDetailServer, *connect.Client) {
	t.Helper()
	srv := connect.NewServer()
	impl := &inProcessDetailServer{richErr: newInProcessRichError(t)}
	impl.register(srv)
	transport := connectinprocess.New(srv, connectinprocess.WithErrorDetailPolicy(policy))
	return impl, connect.NewClient(transport)
}

func callInProcessUnary(t *testing.T, client *connect.Client, procedure string) *connect.Error {
	t.Helper()
	spec := connect.Spec{StreamType: connect.StreamTypeUnary, Procedure: procedure}
	err := client.CallUnary(t.Context(), spec, &emptypb.Empty{}, &emptypb.Empty{})
	var connectErr *connect.Error
	if !assert.True(t, errors.As(err, &connectErr)) {
		t.Fatalf("CallUnary %s: %v", procedure, err)
	}
	return connectErr
}

func callInProcessServerStream(t *testing.T, client *connect.Client, procedure string) *connect.Error {
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

func inProcessDetailTypes(details []*connect.ErrorDetail) []string {
	types := make([]string, 0, len(details))
	for _, detail := range details {
		types = append(types, detail.Type)
	}
	return types
}

// TestInProcessErrorDetailPolicyFilters verifies in-process clients observe
// the same filtered verdict the wire transports produce: the same order,
// duplicates, binary bytes, code, and message.
func TestInProcessErrorDetailPolicyFilters(t *testing.T) {
	t.Parallel()
	policy := connect.NewErrorDetailRules(
		connect.NewErrorDetailRule(inProcessTypeEmpty, inProcessTypeDuration),
	)
	_, client := newInProcessClient(t, policy)

	// Identical expectation to TestErrorDetailPolicyFiltersAcrossProtocols.
	wantTypes := []string{inProcessTypeEmpty, inProcessTypeDuration, inProcessTypeEmpty}
	wantDurationBytes := newInProcessRichError(t).Details()[1].Value

	connectErr := callInProcessUnary(t, client, inProcessUnaryProcedure)
	assert.Equal(t, connectErr.Code(), connect.CodeFailedPrecondition)
	assert.Equal(t, connectErr.Message(), "policy public message")
	assert.Equal(t, inProcessDetailTypes(connectErr.Details()), wantTypes)
	assert.Equal(t, connectErr.Details()[1].Value, wantDurationBytes)
	assert.True(t, connectErr.IsRemote())

	streamErr := callInProcessServerStream(t, client, inProcessStreamProcedure)
	assert.Equal(t, streamErr.Code(), connect.CodeFailedPrecondition)
	assert.Equal(t, inProcessDetailTypes(streamErr.Details()), wantTypes)
	assert.Equal(t, streamErr.Details()[1].Value, wantDurationBytes)
}

// TestInProcessErrorDetailPolicyDisabled confirms the transport keeps its
// existing detail behavior when no policy is configured.
func TestInProcessErrorDetailPolicyDisabled(t *testing.T) {
	t.Parallel()
	srv := connect.NewServer()
	impl := &inProcessDetailServer{richErr: newInProcessRichError(t)}
	impl.register(srv)
	client := connect.NewClient(connectinprocess.New(srv))

	connectErr := callInProcessUnary(t, client, inProcessUnaryProcedure)
	assert.Equal(t, inProcessDetailTypes(connectErr.Details()), []string{
		inProcessTypeEmpty,
		inProcessTypeDuration,
		inProcessTypeEmpty,
		inProcessTypeTimestamp,
	})
}

// TestInProcessErrorDetailPolicyProcedureOverride verifies method-level
// rules take deterministic precedence over the service-level default.
func TestInProcessErrorDetailPolicyProcedureOverride(t *testing.T) {
	t.Parallel()
	policy := connect.NewErrorDetailRules(connect.NewErrorDetailRule(inProcessTypeTimestamp)).
		WithProcedureRule(inProcessUnaryProcedure, connect.NewErrorDetailRule(inProcessTypeEmpty))
	_, client := newInProcessClient(t, policy)

	unaryErr := callInProcessUnary(t, client, inProcessUnaryProcedure)
	assert.Equal(t, inProcessDetailTypes(unaryErr.Details()), []string{
		inProcessTypeEmpty,
		inProcessTypeEmpty,
	})

	streamErr := callInProcessServerStream(t, client, inProcessStreamProcedure)
	assert.Equal(t, inProcessDetailTypes(streamErr.Details()), []string{inProcessTypeTimestamp})
}

// TestInProcessErrorDetailPolicyOriginalUntouched confirms the server-side
// error keeps every detail and its cause, while the client never receives
// the cause or rejected details.
func TestInProcessErrorDetailPolicyOriginalUntouched(t *testing.T) {
	t.Parallel()
	policy := connect.NewErrorDetailRules(connect.NewErrorDetailRule(inProcessTypeEmpty))
	impl, client := newInProcessClient(t, policy)

	connectErr := callInProcessUnary(t, client, inProcessUnaryProcedure)
	assert.Equal(t, inProcessDetailTypes(connectErr.Details()), []string{
		inProcessTypeEmpty,
		inProcessTypeEmpty,
	})
	assert.False(t, errors.Is(connectErr, errInProcessSentinel))

	stashed := impl.stashed.Load()
	assert.NotNil(t, stashed)
	assert.Equal(t, len(stashed.Details()), 4)
	assert.True(t, errors.Is(stashed, errInProcessSentinel))
}

// TestInProcessErrorDetailPolicyRemoteNotLeaked confirms a forwarded remote
// error is scrubbed even with a permissive policy.
func TestInProcessErrorDetailPolicyRemoteNotLeaked(t *testing.T) {
	t.Parallel()
	policy := connect.NewErrorDetailRules(connect.PermitAllErrorDetails())
	_, client := newInProcessClient(t, policy)

	connectErr := callInProcessUnary(t, client, inProcessRemoteProcedure)
	assert.Equal(t, connectErr.Code(), connect.CodeInternal)
	assert.Equal(t, connectErr.Message(), "")
	assert.Zero(t, connectErr.Details())
}

// TestInProcessErrorDetailPolicyPanicAndCancel confirms panic recovery and
// cancellation route through the policy instead of bypassing it.
func TestInProcessErrorDetailPolicyPanicAndCancel(t *testing.T) {
	t.Parallel()
	policy := connect.NewErrorDetailRules(connect.NewErrorDetailRule()) // deny everything
	_, client := newInProcessClient(t, policy)

	panicErr := callInProcessUnary(t, client, inProcessPanicProcedure)
	assert.Equal(t, panicErr.Code(), connect.CodeInternal)
	assert.Zero(t, panicErr.Details())

	cancelErr := callInProcessUnary(t, client, inProcessCancelProcedure)
	assert.Equal(t, cancelErr.Code(), connect.CodeCanceled)
	assert.Zero(t, cancelErr.Details())
}
