/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"planner/builder"
	"router"

	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ Plan = &UnionPlan{}
)

// UnionPlan represents union plan.
type UnionPlan struct {
	log *xlog.Log

	// router
	router *router.Router

	// select ast
	node *sqlparser.Union

	// database
	database string

	// raw query
	RawQuery string

	// type
	typ PlanType

	Root builder.PlanNode
}

// NewUnionPlan used to create SelectPlan.
func NewUnionPlan(log *xlog.Log, database string, query string, node *sqlparser.Union, router *router.Router) *UnionPlan {
	return &UnionPlan{
		log:      log,
		node:     node,
		router:   router,
		database: database,
		RawQuery: query,
		typ:      PlanTypeUnion,
	}
}

// Build used to build distributed querys.
func (p *UnionPlan) Build() error {
	var err error
	p.Root, err = builder.BuildNode(p.log, p.router, p.database, p.node)
	return err
}

// Type returns the type of the plan.
func (p *UnionPlan) Type() PlanType {
	return p.typ
}

// JSON returns the plan info.
func (p *UnionPlan) JSON() string {
	return builder.JSON(p.Root)
}

// Size returns the memory size.
func (p *UnionPlan) Size() int {
	size := len(p.RawQuery)
	return size
}
