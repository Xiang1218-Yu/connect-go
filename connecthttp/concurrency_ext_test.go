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
	"bytes"
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

const admissionHeader = "X-Admission-Entered"

// admissionProtocols covers every wire protocol Mount serves.
func admissionProtocols() []struct {
	name    string
	options []connecthttp.Option
} {
	return []struct {
		name    string
		options []connecthttp.Option
	}{
		{name: connect.ProtocolNameConnect},
		{name: connect.ProtocolNameConnect + "-get", options: []connecthttp.Option{connecthttp.WithHTTPGet()}},
		{name: connect.ProtocolNameGRPC, options: []connecthttp.Option{connecthttp.WithGRPC()}},
		{name: connect.ProtocolNameGRPCWeb, options: []connecthttp.Option{connecthttp.WithGRPCWeb()}},
	}
}

// blockingAdmissionService blocks Ping and CumSum calls on test-controlled
// channels so tests can hold admission slots open deliberately. Lifecycle
// channels and wait groups make handler entry and completion deterministic.
// Every blocking handler stamps a response header on entry, which rejected
// calls must never carry.
type blockingAdmissionService struct {
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

func newBlockingAdmissionService() *blockingAdmissionService {
	return &blockingAdmissionService{
		pingRelease:   make(chan struct{}),
		pingStarted:   make(chan struct{}),
		cumSumRelease: make(chan struct{}),
		cumSumStarted: make(chan struct{}),
	}
}

func (s *blockingAdmissionService) Ping(
	ctx context.Context,
	_ *pingv1.PingRequest,
) (*pingv1.PingResponse, error) {
	if info, ok := connect.CallInfoForServerContext(ctx); ok {
		info.ResponseHeader().Set(admissionHeader, "ping")
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

func (s *blockingAdmissionService) Sum(
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

func (s *blockingAdmissionService) CumSum(
	ctx context.Context,
	stream pingv1connect.PingServiceCumSumServerStream,
) error {
	if info, ok := connect.CallInfoForServerContext(ctx); ok {
		info.ResponseHeader().Set(admissionHeader, "cumsum")
	}
	s.cumSumEntered.Add(1)
	s.cumSumWG.Add(1)
	defer s.cumSumWG.Done()
	// The first message proves the handler owns the stream and its slot.
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

func newAdmissionHTTPServer(
	t *testing.T,
	limits connect.ConcurrencyLimits,
	service pingv1connect.PingServiceHandler,
	handler http.Handler,
) *memhttp.Server {
	t.Helper()
	srv := connect.NewServer()
	srv.SetConcurrencyLimits(limits)
	pingv1connect.RegisterPingServiceHandler(srv, service)
	mux := http.NewServeMux()
	connecthttp.Mount(mux, srv)
	if handler == nil {
		handler = mux
	}
	return memhttptest.NewServer(t, handler)
}

func newAdmissionClient(
	server *memhttp.Server,
	options ...connecthttp.Option,
) pingv1connect.PingServiceClient {
	return pingv1connect.NewPingServiceClient(connect.NewClient(
		connecthttp.NewTransport(server.Client(), server.URL(), options...),
	))
}

func TestAdmissionUnaryRejectedAcrossProtocols(t *testing.T) {
	t.Parallel()
	for _, protocol := range admissionProtocols() {
		t.Run(protocol.name, func(t *testing.T) {
			t.Parallel()
			service := newBlockingAdmissionService()
			server := newAdmissionHTTPServer(t, connect.ConcurrencyLimits{
				MethodLimits: map[string]int{
					pingv1connect.PingServicePingProcedure: 1,
				},
			}, service, nil)
			client := newAdmissionClient(server, protocol.options...)
			ctx := t.Context()

			held := make(chan error, 1)
			go func() {
				_, err := client.Ping(ctx, &pingv1.PingRequest{Number: 1})
				held <- err
			}()
			<-service.pingStarted

			rejectCtx, callInfo := connect.NewClientContext(ctx)
			_, err := client.Ping(rejectCtx, &pingv1.PingRequest{Number: 2})
			assert.Equal(t, connect.CodeOf(err), connect.CodeResourceExhausted)
			assert.Match(t, err.Error(), `procedure ".*Ping"`)
			// The rejected call never ran the handler, so it carries no
			// application response metadata.
			assert.Equal(t, callInfo.ResponseHeader().Get(admissionHeader), "")
			assert.Equal(t, service.pingEntered.Load(), int64(1))

			close(service.pingRelease)
			assert.Nil(t, <-held)
			service.pingWG.Wait()
		})
	}
}

func TestAdmissionServiceLimitRejectsEveryProcedure(t *testing.T) {
	t.Parallel()
	service := newBlockingAdmissionService()
	server := newAdmissionHTTPServer(t, connect.ConcurrencyLimits{
		ServiceLimit: 1,
	}, service, nil)
	client := newAdmissionClient(server)
	ctx := t.Context()

	held := make(chan error, 1)
	go func() {
		_, err := client.Ping(ctx, &pingv1.PingRequest{})
		held <- err
	}()
	<-service.pingStarted

	// A different procedure shares the service-wide slot, so it is rejected
	// with an explainable service-level error before its handler runs.
	stream, err := client.Sum(ctx)
	err = firstStreamError(err, func() error {
		if sendErr := stream.Send(&pingv1.SumRequest{Number: 1}); sendErr != nil {
			return sendErr
		}
		_, closeErr := stream.CloseAndReceive()
		return closeErr
	})
	assert.Equal(t, connect.CodeOf(err), connect.CodeResourceExhausted)
	assert.Match(t, err.Error(), "service concurrency limit")
	assert.Zero(t, service.sumEntered.Load())

	close(service.pingRelease)
	assert.Nil(t, <-held)
	service.pingWG.Wait()

	// The slot is released when the unary call completes.
	sumStream, err := client.Sum(ctx)
	assert.Nil(t, err)
	assert.Nil(t, sumStream.Send(&pingv1.SumRequest{Number: 7}))
	response, err := sumStream.CloseAndReceive()
	assert.Nil(t, err)
	assert.Equal(t, response.GetSum(), int64(7))
}

func TestAdmissionStreamingHoldsSlotUntilStreamEnds(t *testing.T) {
	t.Parallel()
	for _, protocol := range admissionProtocols() {
		// gRPC-Web bidi needs HTTP/2, which the in-memory server speaks via h2c.
		t.Run(protocol.name, func(t *testing.T) {
			t.Parallel()
			service := newBlockingAdmissionService()
			server := newAdmissionHTTPServer(t, connect.ConcurrencyLimits{
				MethodLimits: map[string]int{
					pingv1connect.PingServiceCumSumProcedure: 1,
				},
			}, service, nil)
			client := newAdmissionClient(server, protocol.options...)
			ctx := t.Context()

			heldStream, err := client.CumSum(ctx)
			assert.Nil(t, err)
			assert.Nil(t, heldStream.Send(&pingv1.CumSumRequest{Number: 1}))
			<-service.cumSumStarted

			// While the stream is open, its method slot stays occupied.
			// Bidi rejections are delivered on the read path as
			// ResourceExhausted; when the teardown reaches the client write
			// first, Send instead surfaces a transport-level error. Either
			// way the call is rejected before entering a second handler.
			rejectedStream, err := client.CumSum(ctx)
			assert.Nil(t, err)
			sendErr := rejectedStream.Send(&pingv1.CumSumRequest{Number: 2})
			_, recvErr := rejectedStream.Receive()
			if sendErr != nil {
				// The teardown beat the first write: the read path also
				// reports the terminated stream.
				assert.NotNil(t, recvErr)
			} else {
				assert.Equal(t, connect.CodeOf(recvErr), connect.CodeResourceExhausted)
				assert.Match(t, recvErr.Error(), `procedure ".*CumSum"`)
			}
			assert.Equal(t, service.cumSumEntered.Load(), int64(1))

			// Ending the held stream (half-close plus handler drain) frees
			// the slot for a new stream.
			close(service.cumSumRelease)
			assert.Nil(t, heldStream.CloseSend())
			for {
				if _, err := heldStream.Receive(); err != nil {
					assert.ErrorIs(t, err, io.EOF)
					break
				}
			}
			service.cumSumWG.Wait()

			nextStream, err := client.CumSum(ctx)
			assert.Nil(t, err)
			assert.Nil(t, nextStream.Send(&pingv1.CumSumRequest{Number: 3}))
			assert.Nil(t, nextStream.CloseSend())
			for {
				if _, err := nextStream.Receive(); err != nil {
					assert.ErrorIs(t, err, io.EOF)
					break
				}
			}
		})
	}
}

func TestAdmissionStreamingClientCancellationReleasesSlot(t *testing.T) {
	t.Parallel()
	for _, protocol := range admissionProtocols() {
		t.Run(protocol.name, func(t *testing.T) {
			t.Parallel()
			service := newBlockingAdmissionService()
			server := newAdmissionHTTPServer(t, connect.ConcurrencyLimits{
				MethodLimits: map[string]int{
					pingv1connect.PingServiceCumSumProcedure: 1,
				},
			}, service, nil)
			client := newAdmissionClient(server, protocol.options...)

			streamCtx, cancel := context.WithCancel(t.Context())
			stream, err := client.CumSum(streamCtx)
			assert.Nil(t, err)
			assert.Nil(t, stream.Send(&pingv1.CumSumRequest{Number: 1}))
			<-service.cumSumStarted

			cancel()
			for {
				if _, recvErr := stream.Receive(); recvErr != nil {
					break
				}
			}
			// Cancellation ends the handler, releasing the slot exactly once.
			service.cumSumWG.Wait()

			next, err := client.CumSum(t.Context())
			assert.Nil(t, err)
			assert.Nil(t, next.Send(&pingv1.CumSumRequest{Number: 2}))
			close(service.cumSumRelease)
			assert.Nil(t, next.CloseSend())
			for {
				if _, recvErr := next.Receive(); recvErr != nil {
					assert.ErrorIs(t, recvErr, io.EOF)
					break
				}
			}
		})
	}
}

func TestAdmissionRejectionSkipsRequestBodyReadAndDecompression(t *testing.T) {
	t.Parallel()
	service := newBlockingAdmissionService()
	var bodyReads atomic.Int64
	srv := connect.NewServer()
	srv.SetConcurrencyLimits(connect.ConcurrencyLimits{ServiceLimit: 1})
	pingv1connect.RegisterPingServiceHandler(srv, service)
	mux := http.NewServeMux()
	connecthttp.Mount(mux, srv)
	countingHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = &countingReadCloser{readCloser: r.Body, reads: &bodyReads}
		mux.ServeHTTP(w, r)
	})
	server := memhttptest.NewServer(t, countingHandler)
	client := newAdmissionClient(server)
	ctx := t.Context()

	held := make(chan error, 1)
	go func() {
		_, err := client.Ping(ctx, &pingv1.PingRequest{})
		held <- err
	}()
	<-service.pingStarted
	readsAfterAdmittedCall := bodyReads.Load()

	// A syntactically valid client using gzip would force the server to
	// decompress after admission; the rejection must happen before that.
	gzipClient := newAdmissionClient(server, connecthttp.WithSendGzip(), connecthttp.WithCompressMinBytes(0))
	_, gzipErr := gzipClient.Ping(ctx, &pingv1.PingRequest{})
	assert.Equal(t, connect.CodeOf(gzipErr), connect.CodeResourceExhausted)
	assert.Equal(t, bodyReads.Load(), readsAfterAdmittedCall)

	// A raw request with an invalid gzip payload proves the server never
	// attempted to read or decompress the rejected body: admission wins over
	// the malformed-payload error.
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		server.URL()+pingv1connect.PingServicePingProcedure,
		bytes.NewReader([]byte("definitely-not-gzip")),
	)
	assert.Nil(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	response, err := server.Client().Do(req)
	assert.Nil(t, err)
	defer func() { _ = response.Body.Close() }()
	assert.Equal(t, response.StatusCode, http.StatusTooManyRequests)
	body, err := io.ReadAll(response.Body)
	assert.Nil(t, err)
	assert.Match(t, string(body), "resource_exhausted")
	assert.Equal(t, bodyReads.Load(), readsAfterAdmittedCall)

	close(service.pingRelease)
	assert.Nil(t, <-held)
	service.pingWG.Wait()
}

func TestAdmissionUnlimitedProcedureKeepsOriginalBehavior(t *testing.T) {
	t.Parallel()
	service := newBlockingAdmissionService()
	server := newAdmissionHTTPServer(t, connect.ConcurrencyLimits{
		MethodLimits: map[string]int{
			pingv1connect.PingServicePingProcedure: 1,
		},
	}, service, nil)
	client := newAdmissionClient(server)
	ctx := t.Context()

	held := make(chan error, 1)
	go func() {
		_, err := client.Ping(ctx, &pingv1.PingRequest{})
		held <- err
	}()
	<-service.pingStarted

	// Sum has no method limit and no service limit: many concurrent calls
	// proceed while Ping is saturated.
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
		assert.Nil(t, <-results)
	}

	close(service.pingRelease)
	assert.Nil(t, <-held)
	service.pingWG.Wait()
}

func TestAdmissionReleasesAfterHandlerFailures(t *testing.T) {
	t.Parallel()
	handlerErr := connect.NewError(connect.CodeFailedPrecondition, "handler failure")

	for _, protocol := range admissionProtocols() {
		t.Run(protocol.name, func(t *testing.T) {
			t.Parallel()
			pingServer := &pluggablePingServer{
				ping: func(context.Context, *pingv1.PingRequest) (*pingv1.PingResponse, error) {
					return nil, handlerErr
				},
				countUp: func(
					_ context.Context,
					_ *pingv1.CountUpRequest,
					stream pingv1connect.PingServiceCountUpServerStream,
				) error {
					if err := stream.Send(&pingv1.CountUpResponse{Number: 1}); err != nil {
						return err
					}
					return handlerErr
				},
			}
			server := newAdmissionHTTPServer(t, connect.ConcurrencyLimits{ServiceLimit: 1}, pingServer, nil)
			client := newAdmissionClient(server, protocol.options...)
			ctx := t.Context()

			// Handler errors must release the slot, so every sequential call
			// reaches the handler instead of seeing ResourceExhausted. The
			// wire rebuilds the error as a remote instance, so compare its
			// code and message rather than identity.
			for range 20 {
				_, err := client.Ping(ctx, &pingv1.PingRequest{})
				assert.Equal(t, connect.CodeOf(err), connect.CodeFailedPrecondition)
				assert.Match(t, err.Error(), "handler failure")
			}
			for range 20 {
				stream, err := client.CountUp(ctx, &pingv1.CountUpRequest{Number: 1})
				if err == nil {
					for {
						if _, recvErr := stream.Receive(); recvErr != nil {
							err = recvErr
							break
						}
					}
				}
				assert.Equal(t, connect.CodeOf(err), connect.CodeFailedPrecondition)
				assert.Match(t, err.Error(), "handler failure")
			}
		})
	}
}

func TestAdmissionReconfiguredToZeroDisablesControl(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	var enteredWG sync.WaitGroup
	const concurrent = 4
	enteredWG.Add(concurrent)
	pingServer := &pluggablePingServer{
		ping: func(context.Context, *pingv1.PingRequest) (*pingv1.PingResponse, error) {
			enteredWG.Done()
			<-release
			return &pingv1.PingResponse{}, nil
		},
	}
	srv := connect.NewServer()
	srv.SetConcurrencyLimits(connect.ConcurrencyLimits{
		MethodLimits: map[string]int{
			pingv1connect.PingServicePingProcedure: 1,
		},
	})
	srv.SetConcurrencyLimits(connect.ConcurrencyLimits{})
	pingv1connect.RegisterPingServiceHandler(srv, pingServer)
	mux := http.NewServeMux()
	connecthttp.Mount(mux, srv)
	server := memhttptest.NewServer(t, mux)
	client := newAdmissionClient(server)
	ctx := t.Context()

	results := make(chan error, concurrent)
	for range concurrent {
		go func() {
			_, err := client.Ping(ctx, &pingv1.PingRequest{})
			results <- err
		}()
	}
	// All four calls enter the handler, proving the method limit is gone.
	enteredWG.Wait()
	close(release)
	for range concurrent {
		assert.Nil(t, <-results)
	}
}

// firstStreamError returns err if non-nil, otherwise the error from the first
// stream interaction, since transports may surface admission failures either
// when the stream opens or on its first call.
func firstStreamError(err error, interact func() error) error {
	if err != nil {
		return err
	}
	return interact()
}

// countingReadCloser counts server-side request body reads, proving rejected
// calls never touch their request body.
type countingReadCloser struct {
	readCloser io.ReadCloser
	reads      *atomic.Int64
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	c.reads.Add(1)
	return c.readCloser.Read(p)
}

func (c *countingReadCloser) Close() error {
	return c.readCloser.Close()
}
