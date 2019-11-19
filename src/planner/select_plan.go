/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"errors"

	"planner/builder"
	"router"

	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ Plan = &SelectPlan{}
)

// SelectPlan represents select plan.
type SelectPlan struct {
	log *xlog.Log

	// router
	router *router.Router

	// select ast
	node *sqlparser.Select

	// database
	database string

	// raw query
	RawQuery string

	// type
	typ PlanType

	Root builder.PlanNode
}

// NewSelectPlan used to create SelectPlan.
func NewSelectPlan(log *xlog.Log, database string, query string, node *sqlparser.Select, router *router.Router) *SelectPlan {
	return &SelectPlan{
		log:      log,
		node:     node,
		router:   router,
		database: database,
		RawQuery: query,
		typ:      PlanTypeSelect,
	}
}

// Build used to build distributed querys.
// For now, we don't support subquery in select.
func (p *SelectPlan) Build() error {
	var err error
	// Check subquery.
	if hasSubquery(p.node) {
		return errors.New("unsupported: subqueries.in.select")
	}
	p.Root, err = builder.BuildNode(p.log, p.router, p.database, p.node)
	return err
}

// Type returns the type of the plan.
func (p *SelectPlan) Type() PlanType {
	return p.typ
}

// JSON returns the plan info.
func (p *SelectPlan) JSON() string {
	return builder.JSON(p.Root)
}

// Size returns the memory size.
func (p *SelectPlan) Size() int {
	size := len(p.RawQuery)
	return size
}
