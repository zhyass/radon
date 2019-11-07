/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

import (
	"xcontext"

	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// UnionNode represents union plan.
type UnionNode struct {
	log         *xlog.Log
	Left, Right PlanNode
	// Union Type.
	Typ      string
	children []ChildPlan
	// referred tables' tableInfo map.
	referTables map[string]*tableInfo
}

func newUnionNode(log *xlog.Log, left, right PlanNode, typ string) *UnionNode {
	return &UnionNode{
		log:   log,
		Left:  left,
		Right: right,
		Typ:   typ,
	}
}

// buildQuery used to build the QueryTuple.
func (u *UnionNode) buildQuery(tbInfos map[string]*tableInfo) {
	u.Left.buildQuery(tbInfos)
	u.Right.buildQuery(tbInfos)
}

// Children returns the children of the plan.
func (u *UnionNode) Children() []ChildPlan {
	return u.children
}

// getReferTables get the referTables.
func (u *UnionNode) getReferTables() map[string]*tableInfo {
	return u.referTables
}

// GetQuery used to get the Querys.
func (u *UnionNode) GetQuery() []xcontext.QueryTuple {
	querys := u.Left.GetQuery()
	querys = append(querys, u.Right.GetQuery()...)
	return querys
}

func (u *UnionNode) getFields() []selectTuple {
	return u.Left.getFields()
}

// pushOrderBy used to push the order by exprs.
func (u *UnionNode) pushOrderBy(orderBy sqlparser.OrderBy) error {
	if len(orderBy) > 0 {
		orderPlan := NewOrderByPlan(u.log, orderBy, u.getFields(), u.referTables)
		if err := orderPlan.Build(); err != nil {
			return err
		}
		u.children = append(u.children, orderPlan)
	}
	return nil
}

// pushLimit used to push limit.
func (u *UnionNode) pushLimit(limit *sqlparser.Limit) error {
	limitPlan := NewLimitPlan(u.log, limit)
	u.children = append(u.children, limitPlan)
	return limitPlan.Build()
}

// calcRoute will be called by subquery.
func (u *UnionNode) calcRoute() (PlanNode, error) {
	panic("unreachable")
}

// pushFilter will be called by subquery.
func (u *UnionNode) pushFilter(filter exprInfo) error {
	panic("unreachable")
}

// pushKeyFilter will be called by subquery.
func (u *UnionNode) pushKeyFilter(filter exprInfo, table, field string) error {
	panic("unreachable")
}

// pushSelectExpr will be called by subquery.
func (u *UnionNode) pushSelectExpr(field selectTuple) (int, error) {
	panic("unreachable")
}

// pushHaving will be called by subquery.
func (u *UnionNode) pushHaving(having exprInfo) error {
	panic("unreachable")
}

// pushMisc will be called by subquery.
func (u *UnionNode) pushMisc(sel *sqlparser.Select) {
	panic("unreachable")
}

// addNoTableFilter will be called by subquery.
func (u *UnionNode) addNoTableFilter(exprs []sqlparser.Expr) {
	panic("unreachable")
}

// pushSelectExprs just be called by the processSelect, UnionNode unreachable.
func (u *UnionNode) pushSelectExprs(fields, groups []selectTuple, sel *sqlparser.Select, aggTyp aggrType) error {
	panic("unreachable")
}

// setParent unreachable for UnionNode.
func (u *UnionNode) setParent(p *JoinNode) {
	panic("unreachable")
}

// reOrder unreachable for UnionNode.
func (u *UnionNode) reOrder(int) {
	panic("unreachable")
}

// Order satisfies the plannode interface, unreachable.
func (u *UnionNode) Order() int {
	panic("unreachable")
}
