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
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectinprocess"
	pingv1 "connectrpc.com/connect/v2/internal/gen/connect/ping/v1"
	"connectrpc.com/connect/v2/internal/gen/connect/ping/v1/pingv1connect"
)

const inProcessAdmissionHeader = "X-Admission-Entered"

// inProcessAdmissionService blocks Ping and CumSum on test-controlled
// channels, mirroring the HTTP admission tests so the two transports can be
// proven consistent. Blocking handlers stamp response metadata on entry.
type inProcessAdmissionService struct {
	pingv1connect.UnimplementedPingServiceHandler

	pingEntered atomic.Int64
	pingRelease chan struct{}
	pingStarted chan struct{}
	pingOnce    sync.Once
	pingWG      sync.WaitGroup

	cumSumEntered atomic.Int64
	cumSumRelease chan struct{}
	cumSumStarted chan struct{}
	cumSumOnce    sync.Once
	cumSumWG      sync.WaitGroup

	sumEntered atomic.Int64
}

func newInProcessAdmissionService() *inProcessAdmissionService {
	return &inProcessAdmissionService{
		pingRelease:   make(chan struct{}),
		pingStarted:   make(chan struct{}),
		cumSumRelease: make(chan struct{}),
		cumSumStarted: make(chan struct{}),
	}
}

func (s *inProcessAdmissionService) Ping(
	ctx context.Context,
	_ *pingv1.PingRequest,
) (*pingv1.PingResponse, error) {
	if info, ok := connect.CallInfoForServerContext(ctx); ok {
		info.ResponseHeader().Set(inProcessAdmissionHeader, "ping")
	}
	s.pingEntered.Add(1)
	s.pingWG.Add(1)
	defer s.pingWG.Done()
	s.pingOnce.Do(func() {
		close(s.pingStarted)
	})
	select {
	case <-s.pingRelease:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &pingv1.PingResponse{}, nil
}

func (s *inProcessAdmissionService) Sum(
	_ context.Context,
	stream pingv1connect.PingServiceSumServerStream,
) (*pingv1.SumResponse, error) {
	s.sumEntered.Add(1)
	var sum int64
	for {
		msg, err := stream.Receive()
		if errors.Is(err, io.EOF) {
			return &pingv1.SumResponse{Sum: sum}, nil
		}
		if err != nil {
			return nil, err
		}
		sum += msg.GetNumber()
	}
}

func (s *inProcessAdmissionService) CumSum(
	ctx context.Context,
	stream pingv1connect.PingServiceCumSumServerStream,
) error {
	if info, ok := connect.CallInfoForServerContext(ctx); ok {
		info.ResponseHeader().Set(inProcessAdmissionHeader, "cumsum")
	}
	s.cumSumEntered.Add(1)
	s.cumSumWG.Add(1)
	defer s.cumSumWG.Done()
	if _, err := stream.Receive(); err != nil {
		return err
	}
	s.cumSumOnce.Do(func() {
		close(s.cumSumStarted)
	})
	select {
	case <-s.cumSumRelease:
	case <-ctx.Done():
		return ctx.Err()
	}
	for {
		if _, err := stream.Receive(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func newInProcessAdmissionClient(
	t *testing.T,
	limits connect.ConcurrencyLimits,
	service pingv1connect.PingServiceHandler,
) pingv1connect.PingServiceClient {
	t.Helper()
	server := connect.NewServer()
	server.SetConcurrencyLimits(limits)
	pingv1connect.RegisterPingServiceHandler(server, service)
	return pingv1connect.NewPingServiceClient(connect.NewClient(connectinprocess.New(server)))
}

func TestInProcessAdmissionUnaryRejects(t *testing.T) {
	t.Parallel()
	service := newInProcessAdmissionService()
	client := newInProcessAdmissionClient(t, connect.ConcurrencyLimits{
		MethodLimits: map[string]int{
			pingv1connect.PingServicePingProcedure: 1,
		},
	}, service)
	ctx := t.Context()

	held := make(chan error, 1)
	go func() {
		_, err := client.Ping(ctx, &pingv1.PingRequest{Number: 1})
		held <- err
	}()
	<-service.pingStarted

	rejectCtx, callInfo := connect.NewClientContext(ctx)
	_, err := client.Ping(rejectCtx, &pingv1.PingRequest{Number: 2})
	if connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("rejected Ping code = %s, want ResourceExhausted (err=%v)", connect.CodeOf(err), err)
	}
	if !strings.Contains(err.Error(), `procedure "`+pingv1connect.PingServicePingProcedure+`"`) {
		t.Fatalf("rejection %q does not identify the procedure", err.Error())
	}
	if got := callInfo.ResponseHeader().Get(inProcessAdmissionHeader); got != "" {
		t.Fatalf("rejected call produced response metadata %q", got)
	}
	if got := service.pingEntered.Load(); got != 1 {
		t.Fatalf("handler entered %d times, want 1", got)
	}

	close(service.pingRelease)
	if err := <-held; err != nil {
		t.Fatalf("held Ping: %v", err)
	}
	service.pingWG.Wait()

	// The released slot admits the next call.
	if _, err := client.Ping(ctx, &pingv1.PingRequest{}); err != nil {
		t.Fatalf("Ping after release: %v", err)
	}
}

func TestInProcessAdmissionServiceLimitSpansProcedures(t *testing.T) {
	t.Parallel()
	service := newInProcessAdmissionService()
	client := newInProcessAdmissionClient(t, connect.ConcurrencyLimits{ServiceLimit: 1}, service)
	ctx := t.Context()

	held := make(chan error, 1)
	go func() {
		_, err := client.Ping(ctx, &pingv1.PingRequest{})
		held <- err
	}()
	<-service.pingStarted

	stream, err := client.Sum(ctx)
	if err != nil {
		t.Fatalf("Sum open: %v", err)
	}
	// The server rejects before receiving anything, so the in-process write
	// may end with io.EOF; the verdict itself arrives on CloseAndReceive.
	if err := stream.Send(&pingv1.SumRequest{Number: 1}); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("Sum Send: %v", err)
	}
	_, err = stream.CloseAndReceive()
	if connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("Sum while saturated code = %s, want ResourceExhausted (err=%v)", connect.CodeOf(err), err)
	}
	if !strings.Contains(err.Error(), "service concurrency limit") {
		t.Fatalf("rejection %q does not identify the service limit", err.Error())
	}
	if got := service.sumEntered.Load(); got != 0 {
		t.Fatalf("Sum handler entered %d times, want 0", got)
	}

	close(service.pingRelease)
	if err := <-held; err != nil {
		t.Fatalf("held Ping: %v", err)
	}
	service.pingWG.Wait()

	sumStream, err := client.Sum(ctx)
	if err != nil {
		t.Fatalf("Sum open after release: %v", err)
	}
	if err := sumStream.Send(&pingv1.SumRequest{Number: 7}); err != nil {
		t.Fatalf("Sum Send: %v", err)
	}
	response, err := sumStream.CloseAndReceive()
	if err != nil {
		t.Fatalf("Sum after release: %v", err)
	}
	if response.GetSum() != 7 {
		t.Fatalf("Sum = %d, want 7", response.GetSum())
	}
}

func TestInProcessAdmissionStreamingHoldsUntilStreamEnds(t *testing.T) {
	t.Parallel()
	service := newInProcessAdmissionService()
	client := newInProcessAdmissionClient(t, connect.ConcurrencyLimits{
		MethodLimits: map[string]int{
			pingv1connect.PingServiceCumSumProcedure: 1,
		},
	}, service)
	ctx := t.Context()

	held, err := client.CumSum(ctx)
	if err != nil {
		t.Fatalf("held CumSum open: %v", err)
	}
	if err := held.Send(&pingv1.CumSumRequest{Number: 1}); err != nil {
		t.Fatalf("held CumSum Send: %v", err)
	}
	<-service.cumSumStarted

	rejected, err := client.CumSum(ctx)
	if err != nil {
		t.Fatalf("rejected CumSum open: %v", err)
	}
	if err := rejected.Send(&pingv1.CumSumRequest{Number: 2}); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("rejected CumSum Send: %v", err)
	}
	if _, err := rejected.Receive(); connect.CodeOf(err) != connect.CodeResourceExhausted {
		t.Fatalf("rejected CumSum code = %s, want ResourceExhausted (err=%v)", connect.CodeOf(err), err)
	}
	if got := service.cumSumEntered.Load(); got != 1 {
		t.Fatalf("CumSum handler entered %d times, want 1", got)
	}

	close(service.cumSumRelease)
	if err := held.CloseSend(); err != nil {
		t.Fatalf("held CumSum CloseSend: %v", err)
	}
	for {
		if _, err := held.Receive(); err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("held CumSum terminal error = %v, want EOF", err)
			}
			break
		}
	}
	service.cumSumWG.Wait()

	next, err := client.CumSum(ctx)
	if err != nil {
		t.Fatalf("next CumSum open: %v", err)
	}
	if err := next.Send(&pingv1.CumSumRequest{Number: 3}); err != nil {
		t.Fatalf("next CumSum Send: %v", err)
	}
	if err := next.CloseSend(); err != nil {
		t.Fatalf("next CumSum CloseSend: %v", err)
	}
	if _, err := next.Receive(); !errors.Is(err, io.EOF) {
		t.Fatalf("next CumSum terminal error = %v, want EOF", err)
	}
}

func TestInProcessAdmissionClientCancellationReleases(t *testing.T) {
	t.Parallel()
	service := newInProcessAdmissionService()
	client := newInProcessAdmissionClient(t, connect.ConcurrencyLimits{
		MethodLimits: map[string]int{
			pingv1connect.PingServiceCumSumProcedure: 1,
		},
	}, service)

	streamCtx, cancel := context.WithCancel(t.Context())
	stream, err := client.CumSum(streamCtx)
	if err != nil {
		t.Fatalf("CumSum open: %v", err)
	}
	if err := stream.Send(&pingv1.CumSumRequest{Number: 1}); err != nil {
		t.Fatalf("CumSum Send: %v", err)
	}
	<-service.cumSumStarted

	cancel()
	for {
		if _, err := stream.Receive(); err != nil {
			break
		}
	}
	// The canceled handler must release its slot before a new call is admitted.
	service.cumSumWG.Wait()

	next, err := client.CumSum(t.Context())
	if err != nil {
		t.Fatalf("CumSum after cancel: %v", err)
	}
	if err := next.Send(&pingv1.CumSumRequest{Number: 2}); err != nil {
		t.Fatalf("next CumSum Send: %v", err)
	}
	close(service.cumSumRelease)
	if err := next.CloseSend(); err != nil {
		t.Fatalf("next CumSum CloseSend: %v", err)
	}
	if _, err := next.Receive(); !errors.Is(err, io.EOF) {
		t.Fatalf("next CumSum terminal error = %v, want EOF", err)
	}
}

func TestInProcessAdmissionHandlerErrorReleases(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	handlerErr := connect.NewError(connect.CodeFailedPrecondition, "in-process handler failure")
	service := &pluggableInProcessPingServer{
		ping: func(context.Context, *pingv1.PingRequest) (*pingv1.PingResponse, error) {
			calls.Add(1)
			return nil, handlerErr
		},
	}
	client := newInProcessAdmissionClient(t, connect.ConcurrencyLimits{ServiceLimit: 1}, service)

	// The transport rebuilds the error as a remote instance (causes dropped),
	// so compare code and message: every sequential call must reach the
	// handler rather than see ResourceExhausted.
	for range 50 {
		_, err := client.Ping(t.Context(), &pingv1.PingRequest{})
		if connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Fatalf("Ping code = %s, want FailedPrecondition (err=%v)", connect.CodeOf(err), err)
		}
		if !strings.Contains(err.Error(), "in-process handler failure") {
			t.Fatalf("Ping err = %v, want the handler's message", err)
		}
	}
	if got := calls.Load(); got != 50 {
		t.Fatalf("handler entered %d times, want 50", got)
	}
}

func TestInProcessAdmissionUnlimitedProcedureUnaffected(t *testing.T) {
	t.Parallel()
	service := newInProcessAdmissionService()
	client := newInProcessAdmissionClient(t, connect.ConcurrencyLimits{
		MethodLimits: map[string]int{
			pingv1connect.PingServicePingProcedure: 1,
		},
	}, service)
	ctx := t.Context()

	held := make(chan error, 1)
	go func() {
		_, err := client.Ping(ctx, &pingv1.PingRequest{})
		held <- err
	}()
	<-service.pingStarted

	const concurrent = 5
	results := make(chan error, concurrent)
	for range concurrent {
		go func() {
			stream, err := client.Sum(ctx)
			if err != nil {
				results <- err
				return
			}
			if err := stream.Send(&pingv1.SumRequest{Number: 1}); err != nil {
				results <- err
				return
			}
			_, err = stream.CloseAndReceive()
			results <- err
		}()
	}
	for range concurrent {
		if err := <-results; err != nil {
			t.Fatalf("unlimited Sum: %v", err)
		}
	}

	close(service.pingRelease)
	if err := <-held; err != nil {
		t.Fatalf("held Ping: %v", err)
	}
}

func TestInProcessAdmissionZeroConfigDisables(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	var enteredWG sync.WaitGroup
	const concurrent = 4
	enteredWG.Add(concurrent)
	service := &pluggableInProcessPingServer{
		ping: func(context.Context, *pingv1.PingRequest) (*pingv1.PingResponse, error) {
			enteredWG.Done()
			<-release
			return &pingv1.PingResponse{}, nil
		},
	}
	server := connect.NewServer()
	server.SetConcurrencyLimits(connect.ConcurrencyLimits{ServiceLimit: 1})
	server.SetConcurrencyLimits(connect.ConcurrencyLimits{})
	pingv1connect.RegisterPingServiceHandler(server, service)
	client := pingv1connect.NewPingServiceClient(connect.NewClient(connectinprocess.New(server)))

	results := make(chan error, concurrent)
	for range concurrent {
		go func() {
			_, err := client.Ping(t.Context(), &pingv1.PingRequest{})
			results <- err
		}()
	}
	enteredWG.Wait()
	close(release)
	for range concurrent {
		if err := <-results; err != nil {
			t.Fatalf("Ping: %v", err)
		}
	}
}

// pluggableInProcessPingServer is a PingServiceHandler with only Ping
// overridden; streaming methods fall back to Unimplemented behavior.
type pluggableInProcessPingServer struct {
	pingv1connect.UnimplementedPingServiceHandler

	ping func(context.Context, *pingv1.PingRequest) (*pingv1.PingResponse, error)
}

func (s *pluggableInProcessPingServer) Ping(
	ctx context.Context,
	req *pingv1.PingRequest,
) (*pingv1.PingResponse, error) {
	return s.ping(ctx, req)
}
