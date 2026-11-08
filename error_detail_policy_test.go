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

import (
	"errors"
	"testing"

	"connectrpc.com/connect/v2/internal/assert"
)

const (
	policyDetailTypeA = "acme.policy.v1.DetailA"
	policyDetailTypeB = "acme.policy.v1.DetailB"
	policyDetailTypeC = "acme.policy.v1.DetailC"
)

func policySpec(procedure string) Spec {
	return Spec{Procedure: procedure}
}

func TestErrorDetailRule(t *testing.T) {
	t.Parallel()
	t.Run("zero value permits nothing", func(t *testing.T) {
		t.Parallel()
		var rule ErrorDetailRule
		assert.False(t, rule.permits(policyDetailTypeA))
	})
	t.Run("allowlist", func(t *testing.T) {
		t.Parallel()
		rule := NewErrorDetailRule(policyDetailTypeA, policyDetailTypeB, policyDetailTypeA)
		assert.True(t, rule.permits(policyDetailTypeA))
		assert.True(t, rule.permits(policyDetailTypeB))
		assert.False(t, rule.permits(policyDetailTypeC))
	})
	t.Run("empty allowlist permits nothing", func(t *testing.T) {
		t.Parallel()
		assert.False(t, NewErrorDetailRule().permits(policyDetailTypeA))
	})
	t.Run("permit all", func(t *testing.T) {
		t.Parallel()
		rule := PermitAllErrorDetails()
		assert.True(t, rule.permits(policyDetailTypeA))
		assert.True(t, rule.permits(policyDetailTypeC))
	})
}

func TestErrorDetailRulesPrecedence(t *testing.T) {
	t.Parallel()
	const (
		defaultProc  = "/acme.policy.v1.PolicyService/Default"
		overrideProc = "/acme.policy.v1.PolicyService/Override"
	)
	rules := NewErrorDetailRules(NewErrorDetailRule(policyDetailTypeA)).
		WithProcedureRule(overrideProc, NewErrorDetailRule(policyDetailTypeB))

	spec := policySpec(defaultProc)
	assert.True(t, rules.PermitErrorDetail(spec, policyDetailTypeA))
	assert.False(t, rules.PermitErrorDetail(spec, policyDetailTypeB))

	overrideSpec := policySpec(overrideProc)
	assert.False(t, rules.PermitErrorDetail(overrideSpec, policyDetailTypeA))
	assert.True(t, rules.PermitErrorDetail(overrideSpec, policyDetailTypeB))

	// An override replaces rather than merges the default rule.
	permitAll := NewErrorDetailRules(NewErrorDetailRule(policyDetailTypeA)).
		WithProcedureRule(overrideProc, PermitAllErrorDetails())
	assert.True(t, permitAll.PermitErrorDetail(overrideSpec, policyDetailTypeC))

	// A nil rules value denies rather than panicking.
	var nilRules *ErrorDetailRules
	assert.False(t, nilRules.PermitErrorDetail(spec, policyDetailTypeA))
}

func TestErrorDetailPolicyFunc(t *testing.T) {
	t.Parallel()
	var seenSpec Spec
	var seenType string
	policy := ErrorDetailPolicyFunc(func(spec Spec, detailType string) bool {
		seenSpec = spec
		seenType = detailType
		return detailType == policyDetailTypeA
	})
	spec := policySpec("/acme.policy.v1.PolicyService/Call")
	assert.True(t, policy.PermitErrorDetail(spec, policyDetailTypeA))
	assert.Equal(t, seenSpec, spec)
	assert.Equal(t, seenType, policyDetailTypeA)
	assert.False(t, policy.PermitErrorDetail(spec, policyDetailTypeB))
}

func TestFilterErrorDetailsDisabled(t *testing.T) {
	t.Parallel()
	err := NewError(CodeInternal, "boom").
		WithDetail(&ErrorDetail{Type: policyDetailTypeA, Value: []byte{1, 2, 3}})
	// A nil policy disables filtering and returns the error unchanged.
	assert.True(t, FilterErrorDetails(nil, policySpec("proc"), err) == err)
	// An error without details is also returned unchanged.
	plain := NewError(CodeInternal, "boom")
	permitAll := NewErrorDetailRules(PermitAllErrorDetails())
	assert.True(t, FilterErrorDetails(permitAll, policySpec("proc"), plain) == plain)
	assert.Nil(t, FilterErrorDetails(nil, policySpec("proc"), nil))
}

func TestFilterErrorDetailsKeepsOrderDuplicatesAndBinary(t *testing.T) {
	t.Parallel()
	first := &ErrorDetail{Type: policyDetailTypeA, Value: []byte{0x00, 0x01, 0xff}, Debug: []byte("debug-a")}
	second := &ErrorDetail{Type: policyDetailTypeB, Value: []byte{0x0a}}
	duplicate := &ErrorDetail{Type: policyDetailTypeA, Value: []byte{0x00, 0x01, 0xff}, Debug: []byte("debug-a")}
	err := NewError(CodePermissionDenied, "public message").
		WithDetail(first).
		WithDetail(second).
		WithDetail(duplicate)

	rules := NewErrorDetailRules(NewErrorDetailRule(policyDetailTypeA))
	filtered := FilterErrorDetails(rules, policySpec("/acme.policy.v1.PolicyService/Call"), err)

	// Code and public message survive even though type B is rejected.
	assert.Equal(t, filtered.Code(), CodePermissionDenied)
	assert.Equal(t, filtered.Message(), "public message")
	got := filtered.Details()
	assert.Equal(t, len(got), 2)
	// Order, duplicates, and the exact binary/debug bytes are preserved.
	assert.Equal(t, got[0], first)
	assert.Equal(t, got[1], duplicate)
	assert.Equal(t, got[0].Value, first.Value)
	assert.Equal(t, got[1].Debug, duplicate.Debug)
}

func TestFilterErrorDetailsRejectsAll(t *testing.T) {
	t.Parallel()
	err := NewError(CodeResourceExhausted, "slow down").
		WithDetail(&ErrorDetail{Type: policyDetailTypeA, Value: []byte("x")}).
		WithDetail(&ErrorDetail{Type: policyDetailTypeB, Value: []byte("y")})
	rules := NewErrorDetailRules(NewErrorDetailRule(policyDetailTypeC))
	filtered := FilterErrorDetails(rules, policySpec("/acme.policy.v1.PolicyService/Call"), err)
	assert.Zero(t, filtered.Details())
	// Code and message must remain even when every detail is filtered.
	assert.Equal(t, filtered.Code(), CodeResourceExhausted)
	assert.Equal(t, filtered.Message(), "slow down")
}

func TestFilterErrorDetailsDoesNotMutateOriginal(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("local-only cause")
	detail := &ErrorDetail{Type: policyDetailTypeA, Value: []byte("secret")}
	err := NewError(CodeInternal, "public").WithDetail(detail).WithCause(sentinel)

	rules := NewErrorDetailRules(NewErrorDetailRule(policyDetailTypeB))
	filtered := FilterErrorDetails(rules, policySpec("/acme.policy.v1.PolicyService/Call"), err)

	// The client-visible error has no details.
	assert.Zero(t, filtered.Details())
	// The handler's original error keeps every detail and its local cause.
	assert.Equal(t, len(err.Details()), 1)
	assert.Equal(t, err.Details()[0], detail)
	assert.True(t, errors.Is(err, sentinel))
	// The filtered clone never carries the cause onto the wire path: the
	// cause remains a local-only field that marshalers do not read.
	assert.True(t, errors.Is(filtered, sentinel))
}

func TestFilterErrorDetailsPermitsAllReturnsOriginal(t *testing.T) {
	t.Parallel()
	err := NewError(CodeInternal, "boom").
		WithDetail(&ErrorDetail{Type: policyDetailTypeA, Value: []byte("x")})
	policy := NewErrorDetailRules(PermitAllErrorDetails())
	assert.True(t, FilterErrorDetails(policy, policySpec("proc"), err) == err)
}
