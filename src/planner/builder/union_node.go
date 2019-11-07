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

	"github.com/pkg/errors"
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
	referTables             map[string]*tableInfo
	leftColMap, rightColMap map[string]selectTuple
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

func (u *UnionNode) pushSelectExprs(fields, groups []selectTuple, sel *sqlparser.Select, aggTyp aggrType) error {
	panic("unreachable")
}

// 左右都要往里面push，首先要把field替换掉
func (u *UnionNode) pushSelectExpr(field selectTuple) (int, error) {
	var lidx, ridx int
	var newField selectTuple
	var err error

	if lidx, err = handleSelectExpr(field, u.Left); err != nil {
		return -1, err
	}

	expr := sqlparser.CloneSelectExpr(field.expr)
	newField = parserExpr(expr.(*sqlparser.AliasedExpr).Expr)
	if field.alias != "" {
		newField.expr.(*sqlparser.AliasedExpr).As = sqlparser.NewColIdent(field.alias)
		newField.alias = field.alias
	}

	if newField, err = replaceSelect(newField, u.rightColMap); err != nil {
		return -1, err
	}
	if ridx, err = handleSelectExpr(newField, u.Right); err != nil {
		return -1, err
	}

	if lidx != ridx {
		return -1, err
	}
	return lidx, nil
}

func (u *UnionNode) pushFilter(filter exprInfo) error {
	var err error
	if err = handleFilter(filter, u.Left); err != nil {
		return err
	}

	expr := sqlparser.CloneExpr(filter.expr)
	newInfo := exprInfo{expr, nil, fetchCols(expr), nil}
	newInfo, err = replaceCol(newInfo, u.rightColMap)
	if err != nil {
		return err
	}

	if len(newInfo.referTables) == 0 {
		return errors.New("unsupport: cannot.push.where.clause.into.'dual'.table")
	}
	return handleFilter(newInfo, u.Right)
}

func (u *UnionNode) calcRoute() (PlanNode, error) {
	var err error
	if u.Left, err = u.Left.calcRoute(); err != nil {
		return nil, err
	}
	if u.Right, err = u.Right.calcRoute(); err != nil {
		return nil, err
	}

	return u, nil
}

func (u *UnionNode) setParent(p *JoinNode) {
	panic("unreachable")
}

func (u *UnionNode) pushHaving(filter exprInfo) error {
	var err error
	if err = handleHaving(filter, u.Left); err != nil {
		return err
	}

	expr := sqlparser.CloneExpr(filter.expr)
	newInfo := exprInfo{expr, nil, fetchCols(expr), nil}
	newInfo, err = replaceCol(newInfo, u.rightColMap)
	if err != nil {
		return err
	}
	if len(newInfo.referTables) == 0 {
		return errors.New("unsupport: cannot.push.having.clause.into.'dual'.table")
	}
	return handleHaving(newInfo, u.Right)
}

func (u *UnionNode) pushKeyFilter(filter exprInfo, table, field string) error {
	expr := sqlparser.CloneExpr(filter.expr)
	newInfo := exprInfo{expr, []string{table}, fetchCols(expr), nil}
	return u.pushFilter(newInfo)
}

func (u *UnionNode) addNoTableFilter(exprs []sqlparser.Expr) {
	u.Left.addNoTableFilter(exprs)
	u.Right.addNoTableFilter(exprs)
}

func (u *UnionNode) pushMisc(sel *sqlparser.Select) {
	u.Left.pushMisc(sel)
	u.Right.pushMisc(sel)
}

// reOrder unreachable for UnionNode.
func (u *UnionNode) reOrder(int) {
	panic("unreachable")
}

// Order satisfies the plannode interface, unreachable.
func (u *UnionNode) Order() int {
	panic("unreachable")
}
