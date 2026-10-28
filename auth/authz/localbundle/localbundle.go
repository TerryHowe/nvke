// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package localbundle is the LocalBundle authorization backend. It evaluates
// Cedar policy from a bundle of files the deployer mounts, usually from a
// ConfigMap: the policies, the roles and the actions.
//
// LocalBundle gives no caller a tenant and grants no caller a role, so it
// allows only a request for a global resource that a policy permits without
// a role.
package localbundle

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/cedar-policy/cedar-go"
	"github.com/cedar-policy/cedar-go/types"

	"github.com/dsx-ai-factory/nvke/auth/authz"
)

// The Cedar names a policy uses. Every entity type is in the NVKE namespace:
// a Resource of Type "Cluster" has the entity type NVKE::Cluster.
const (
	namespace     = "NVKE"
	principalType = namespace + "::Principal"
	actionType    = namespace + "::Action"
	roleType      = namespace + "::Role"

	// contextIssuer is the one attribute of the Cedar context: the name of
	// the caller's issuer in the authN config.
	contextIssuer = "issuer"
)

// Options configures a LocalBundle.
type Options struct {
	// Dir holds the bundle files: policies.cedar, roles.json and
	// actions.json.
	Dir string

	// Actions are the actions the API asks about. New fails if actions.json
	// lacks one, because the bundle would refuse that action to every caller,
	// or makes one an action group. Authorize refuses any other action.
	Actions []string

	// ResourceTypes are the types of the resources the API asks about, such
	// as "Cluster" and "Collection". A policy may name only these, the
	// principal, actions and roles, so a misspelled type can't make a forbid
	// silently never apply.
	ResourceTypes []string
}

// LocalBundle is a backend that evaluates one loaded bundle. It is safe for
// concurrent use: the bundle never changes after New.
type LocalBundle struct {
	bundle *bundle
}

var _ authz.Authorizer = (*LocalBundle)(nil)

// New loads and checks the bundle in opts.Dir. Unless a file can't be read
// or parsed, the error reports every problem found.
func New(opts Options) (*LocalBundle, error) {
	if len(opts.Actions) == 0 || len(opts.ResourceTypes) == 0 {
		return nil, errors.New("the API's actions and resource types must both be set")
	}
	for _, t := range opts.ResourceTypes {
		if t == "" || slices.Contains([]string{principalType, actionType, roleType}, namespace+"::"+t) {
			return nil, fmt.Errorf("resource type %q is empty or names the principal, an action or a role", t)
		}
	}
	b, err := load(opts)
	if err != nil {
		return nil, err
	}
	return &LocalBundle{bundle: b}, nil
}

// Name returns "localBundle".
func (*LocalBundle) Name() string { return "localBundle" }

// Ready reports whether a bundle is loaded.
func (lb *LocalBundle) Ready(context.Context) error {
	if lb == nil || lb.bundle == nil {
		return errors.New("no bundle is loaded")
	}
	return nil
}

// Authorize runs the isolation check, then evaluates the policies.
func (lb *LocalBundle) Authorize(ctx context.Context, q authz.Query) (authz.Decision, error) {
	if err := lb.Ready(ctx); err != nil {
		return authz.Decision{}, err
	}
	if err := q.Validate(); err != nil {
		return authz.Decision{}, err
	}
	// Every caller has no tenant and isn't an operator.
	var caller authz.Caller
	if refusal := authz.CheckIsolation(caller, q.Resource); refusal != "" {
		return authz.Decision{Outcome: refusal, Caller: caller}, nil
	}
	return lb.bundle.decide(q)
}

// decide evaluates q against the policies. The principal holds no roles.
func (b *bundle) decide(q authz.Query) (authz.Decision, error) {
	if !slices.Contains(b.resourceTypes, q.Resource.Type) {
		return authz.Decision{}, fmt.Errorf("resource type %q isn't one of the API's resource types", q.Resource.Type)
	}
	if !slices.Contains(b.actions, q.Action) {
		return authz.Decision{}, fmt.Errorf("action %q isn't one of the API's actions", q.Action)
	}
	action := types.NewEntityUID(actionType, types.String(q.Action))

	principal := types.NewEntityUID(principalType, types.String(q.Principal.Subject))
	resource := types.NewEntityUID(types.EntityType(namespace+"::"+q.Resource.Type), types.String(q.Resource.ID))
	request := cedar.Request{
		Principal: principal,
		Action:    action,
		Resource:  resource,
		Context:   types.NewRecord(types.RecordMap{contextIssuer: types.String(q.Principal.IssuerName)}),
	}
	view := requestEntities{
		stored:    b.entities,
		principal: types.Entity{UID: principal, Parents: types.NewEntityUIDSet(), Attributes: types.NewRecord(nil)},
		resource:  types.Entity{UID: resource, Parents: types.NewEntityUIDSet(), Attributes: types.NewRecord(nil)},
	}
	decision, diagnostic := cedar.Authorize(b.policies, view, request)

	// Cedar skips a policy whose condition errors. An allow beside an error
	// may be one a skipped forbid would have stopped. A deny stays a deny.
	if decision == cedar.Allow && len(diagnostic.Errors) > 0 {
		msgs := make([]string, 0, len(diagnostic.Errors))
		for _, e := range diagnostic.Errors {
			msgs = append(msgs, fmt.Sprintf("policy %s failed to evaluate: %s", b.policyName(e.PolicyID), e.Message))
		}
		slices.Sort(msgs)
		return authz.Decision{}, errors.New(strings.Join(msgs, "\n"))
	}
	d := authz.Decision{Outcome: authz.OutcomeDeny}
	if decision == cedar.Allow {
		d.Outcome = authz.OutcomeAllow
	}
	for _, reason := range diagnostic.Reasons {
		d.Reasons = append(d.Reasons, b.policyName(reason.PolicyID))
	}
	slices.Sort(d.Reasons)
	return d, nil
}

// requestEntities is the stored entities plus the request's principal and
// resource, without copying the stored map, which every request shares.
// Neither can replace a stored entity: stored entities are actions and roles,
// and the resource type can be neither.
type requestEntities struct {
	stored    types.EntityMap
	principal types.Entity
	resource  types.Entity
}

// Get returns the request's principal or resource, and looks up any other
// entity in the bundle.
func (e requestEntities) Get(uid types.EntityUID) (types.Entity, bool) {
	switch uid {
	case e.principal.UID:
		return e.principal, true
	case e.resource.UID:
		return e.resource, true
	default:
		return e.stored.Get(uid)
	}
}
