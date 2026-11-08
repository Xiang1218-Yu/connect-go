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

// ErrorDetailPolicy decides which of an [*Error]'s details a server may
// send to a client. Server transports apply the policy after a handler
// error has been scrubbed and immediately before it is serialized, so the
// same decision governs the Connect, gRPC, gRPC-Web, and in-process
// transports.
//
// The policy decides by detail type (see [ErrorDetail.Type]); it never sees
// the detail's binary value. Details a policy permits keep their original
// order, duplicates, and binary content. Details it rejects are omitted from
// the client-visible error while the error's code and public message are
// preserved.
//
// Applying a policy never mutates the handler's original error: handlers and
// interceptors can keep using the returned error for local logging and
// errors.Is/errors.As checks. Causes, response headers, trailers, and remote
// errors are never disclosed through details.
//
// Implementations must be safe to call concurrently.
type ErrorDetailPolicy interface {
	// PermitErrorDetail reports whether a detail of detailType may be
	// included in the wire error for the RPC described by spec.
	PermitErrorDetail(spec Spec, detailType string) bool
}

// ErrorDetailPolicyFunc adapts an ordinary function to an [ErrorDetailPolicy].
type ErrorDetailPolicyFunc func(spec Spec, detailType string) bool

// PermitErrorDetail implements [ErrorDetailPolicy].
func (f ErrorDetailPolicyFunc) PermitErrorDetail(spec Spec, detailType string) bool {
	return f(spec, detailType)
}

// ErrorDetailRule is an allowlist of error detail message types. The zero
// value permits no details, which is the safe choice for a rule configured
// explicitly. Build rules with [NewErrorDetailRule] or
// [PermitAllErrorDetails].
type ErrorDetailRule struct {
	permitAll bool
	allowed   map[string]struct{}
}

// NewErrorDetailRule returns a rule that permits only the given fully
// qualified detail message types, such as "google.rpc.RetryInfo". Repeated
// types are deduplicated. With no arguments, the rule permits nothing.
func NewErrorDetailRule(detailTypes ...string) ErrorDetailRule {
	rule := ErrorDetailRule{allowed: make(map[string]struct{}, len(detailTypes))}
	for _, detailType := range detailTypes {
		rule.allowed[detailType] = struct{}{}
	}
	return rule
}

// PermitAllErrorDetails returns a rule that permits every detail type.
func PermitAllErrorDetails() ErrorDetailRule {
	return ErrorDetailRule{permitAll: true}
}

// permits reports whether detailType is covered by the rule.
func (r ErrorDetailRule) permits(detailType string) bool {
	if r.permitAll {
		return true
	}
	_, ok := r.allowed[detailType]
	return ok
}

// ErrorDetailRules is an [ErrorDetailPolicy] that combines a service-level
// default rule with per-procedure overrides. When an RPC has an override for
// its [Spec.Procedure], the override replaces the default entirely; otherwise
// the default applies. This gives method-level rules deterministic precedence
// over service-level rules.
//
// ErrorDetailRules values are intended for setup-time configuration and must
// not be changed while an RPC is in flight.
type ErrorDetailRules struct {
	defaultRule ErrorDetailRule
	overrides   map[string]ErrorDetailRule
}

// NewErrorDetailRules returns an [ErrorDetailRules] that applies defaultRule
// to every RPC without a procedure-specific override.
func NewErrorDetailRules(defaultRule ErrorDetailRule) *ErrorDetailRules {
	return &ErrorDetailRules{defaultRule: defaultRule}
}

// WithProcedureRule returns r with rule installed for procedure, which must
// match [Spec.Procedure] (for example,
// "/acme.user.v1.UserService/GetUser"). The rule replaces, rather than
// merges with, the service-level default for that procedure.
func (r *ErrorDetailRules) WithProcedureRule(procedure string, rule ErrorDetailRule) *ErrorDetailRules {
	if r.overrides == nil {
		r.overrides = make(map[string]ErrorDetailRule)
	}
	r.overrides[procedure] = rule
	return r
}

// PermitErrorDetail implements [ErrorDetailPolicy]. A procedure override
// wins when present; the service-level default otherwise.
func (r *ErrorDetailRules) PermitErrorDetail(spec Spec, detailType string) bool {
	if r == nil {
		return false
	}
	if rule, ok := r.overrides[spec.Procedure]; ok {
		return rule.permits(detailType)
	}
	return r.defaultRule.permits(detailType)
}

// FilterErrorDetails returns the [*Error] that may cross the wire for the RPC
// in spec under policy. The returned error preserves err's code and public
// message and carries only the details policy permits, in their original
// order with duplicates and binary content intact. When policy permits every
// detail or rejects none, err is returned unchanged.
//
// The original err is never modified: its full detail list, cause, and remote
// marker remain available for local logging and error checks. If policy is
// nil, filtering is disabled and err is returned unchanged, preserving the
// default transport behavior.
func FilterErrorDetails(policy ErrorDetailPolicy, spec Spec, err *Error) *Error {
	if policy == nil || err == nil {
		return err
	}
	details := err.Details()
	if len(details) == 0 {
		return err
	}
	filtered := make([]*ErrorDetail, 0, len(details))
	for _, detail := range details {
		if policy.PermitErrorDetail(spec, detail.Type) {
			filtered = append(filtered, detail)
		}
	}
	if len(filtered) == len(details) {
		// Nothing was rejected: keep the original error and avoid an
		// allocation.
		return err
	}
	clone := *err
	// A nil list matches the representation of a freshly constructed
	// [*Error]; the marshalers treat nil and empty identically.
	clone.details = filtered
	if len(filtered) == 0 {
		clone.details = nil
	}
	return &clone
}
