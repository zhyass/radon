/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

import (
	"xcontext"

	"github.com/xelabs/go-mysqlstack/sqlparser"
)

// PlanNode interface.
type PlanNode interface {
	buildQuery(tbInfos map[string]*tableInfo)
	calcRoute() (PlanNode, error)
	Children() []ChildPlan
	getFields() []selectTuple
	getReferTables() map[string]*tableInfo
	GetQuery() []xcontext.QueryTuple
	pushSelectExprs(fields, groups []selectTuple, sel *sqlparser.Select, aggTyp aggrType) error
	pushOrderBy(orderBy sqlparser.OrderBy) error
	pushLimit(limit *sqlparser.Limit) error
	pushMisc(sel *sqlparser.Select)
	pushFilter(filter exprInfo) error
	pushKeyFilter(filter exprInfo, table, field string) error
	pushSelectExpr(field selectTuple) (int, error)
	pushHaving(having exprInfo) error
	reOrder(int)
	setParent(p *JoinNode)
	setNoTableFilter(exprs []sqlparser.Expr)
	Order() int
}

// findLCA get the two plannode's lowest common ancestors node.
func findLCA(h, p1, p2 PlanNode) PlanNode {
	if p1 == h || p2 == h {
		return h
	}
	jn, ok := h.(*JoinNode)
	if !ok {
		return nil
	}
	pl := findLCA(jn.Left, p1, p2)
	pr := findLCA(jn.Right, p1, p2)

	if pl != nil && pr != nil {
		return jn
	}
	if pl == nil {
		return pr
	}
	return pl
}

func setParenthese(node PlanNode, hasParen bool) {
	switch node := node.(type) {
	case *JoinNode:
		node.hasParen = hasParen
	case *MergeNode:
		node.hasParen = hasParen
	}
}

func findParent(tables []string, node PlanNode) PlanNode {
	var parent PlanNode
	for _, tb := range tables {
		tbInfo := node.getReferTables()[tb]
		if parent == nil {
			parent = tbInfo.parent
			continue
		}
		if parent != tbInfo.parent {
			parent = findLCA(node, parent, tbInfo.parent)
		}
	}
	return parent
}

func handleFilter(s PlanNode, filter exprInfo) {
	switch node := s.(type) {
	case *JoinNode:
		node.otherFilter = append(node.otherFilter, filter)
	case *MergeNode:
		node.addWhere(filter.expr)
	}
}

func pushFilters(s PlanNode, expr sqlparser.Expr) (PlanNode, error) {
	joins, filters, err := parserWhereOrJoinExprs(expr, s.getReferTables())
	if err != nil {
		return s, err
	}

	for _, filter := range filters {
		if err := s.pushFilter(filter); err != nil {
			return s, err
		}
	}

	switch node := s.(type) {
	case *MergeNode:
		for _, joinFilter := range joins {
			node.addWhere(joinFilter.expr)
		}
	case *JoinNode:
		return node.pushEqualCmpr(joins), nil
	}
	return s, nil
}

func pushHavings(s PlanNode, expr sqlparser.Expr, tbInfos map[string]*tableInfo) error {
	havings, err := parserHaving(expr, tbInfos, s.getFields())
	if err != nil {
		return err
	}
	for _, having := range havings {
		if err = s.pushHaving(having); err != nil {
			return err
		}
	}
	return nil
}
