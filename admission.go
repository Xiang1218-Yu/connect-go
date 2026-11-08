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

package connect

import "sync"

// ConcurrencyLimits configures optional concurrency admission control for a
// [Server]. The zero value disables admission control, so calls keep their
// original behavior.
//
// Admission is decided before a call enters its handler: a call that exceeds
// a limit fails immediately with [CodeResourceExhausted], without reading its
// first request message, decompressing any payload, or producing response
// metadata.
type ConcurrencyLimits struct {
	// ServiceLimit caps the number of concurrently active calls across every
	// procedure on the server. A non-positive value disables the service-wide
	// limit. The limit applies to every call, including calls to procedures
	// that are not registered.
	ServiceLimit int

	// MethodLimits caps the number of concurrently active calls for a single
	// procedure, keyed by [Spec.Procedure]. Procedures without an entry, or
	// with a non-positive value, are not limited independently. A method limit
	// may be stricter than ServiceLimit: every admitted call holds one
	// service-wide slot and, when configured, one method slot.
	MethodLimits map[string]int
}

// SetConcurrencyLimits installs concurrency admission control on the server.
// Passing the zero value removes any previously configured limits.
//
// When both a service-wide and a method limit apply, the service-wide slot is
// reserved first, then the method slot. A call rejected by the method limit
// releases the service-wide slot it reserved, so the two limits never leak
// capacity into each other. Rejections always report [CodeResourceExhausted],
// with a message identifying whether the service-wide or the method limit was
// reached.
//
// Admitted calls release their slots when [Server.Call] returns: unary calls
// when the handler completes, and streaming calls only once the stream truly
// ends. Cancellation, handler errors, protocol failures, and panics all
// release the slots exactly once.
//
// SetConcurrencyLimits is intended for setup and must not run concurrently
// with [Server.Call].
func (s *Server) SetConcurrencyLimits(limits ConcurrencyLimits) {
	s.admission = newAdmissionController(limits)
}

// admissionController enforces the optional service-wide and per-procedure
// active-call limits. Each limit is a buffered channel used as a non-blocking
// semaphore, so admit never queues callers: an exhausted limit rejects the
// call immediately. A nil controller admits every call.
type admissionController struct {
	service chan struct{}            // nil when the service-wide limit is disabled
	methods map[string]chan struct{} // procedure -> semaphore; only positive limits are present
}

func newAdmissionController(limits ConcurrencyLimits) *admissionController {
	controller := &admissionController{}
	if limits.ServiceLimit > 0 {
		controller.service = make(chan struct{}, limits.ServiceLimit)
	}
	for procedure, limit := range limits.MethodLimits {
		if limit <= 0 {
			continue
		}
		if controller.methods == nil {
			controller.methods = make(map[string]chan struct{})
		}
		controller.methods[procedure] = make(chan struct{}, limit)
	}
	if controller.service == nil && len(controller.methods) == 0 {
		return nil
	}
	return controller
}

// admit reserves slots for a call to procedure without blocking. On success
// it returns a lease that must be released exactly once. On failure no slots
// are held and the returned [*Error] explains which limit rejected the call.
func (c *admissionController) admit(procedure string) (*admissionLease, *Error) {
	if c == nil {
		return nil, nil
	}
	lease := &admissionLease{}
	if c.service != nil {
		select {
		case c.service <- struct{}{}:
			lease.service = c.service
		default:
			return nil, Errorf(
				CodeResourceExhausted,
				"service concurrency limit of %d active calls exceeded",
				cap(c.service),
			)
		}
	}
	if method, ok := c.methods[procedure]; ok {
		select {
		case method <- struct{}{}:
			lease.method = method
		default:
			// Roll back the service-wide slot so a method-level rejection
			// never consumes service-wide capacity.
			lease.release()
			return nil, Errorf(
				CodeResourceExhausted,
				"concurrency limit of %d active calls exceeded for procedure %q",
				cap(method),
				procedure,
			)
		}
	}
	return lease, nil
}

// admissionLease holds the semaphore slots owned by one admitted call. release
// is idempotent, so every call path can rely on it running exactly once.
type admissionLease struct {
	once    sync.Once
	service chan struct{}
	method  chan struct{}
}

// release returns the method slot first, then the service-wide slot. It is
// safe to call multiple times.
func (l *admissionLease) release() {
	l.once.Do(func() {
		if l.method != nil {
			<-l.method
		}
		if l.service != nil {
			<-l.service
		}
	})
}
