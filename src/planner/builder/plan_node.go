/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

import (
	"encoding/json"

	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
)

// PlanNode interface.
type PlanNode interface {
	buildQuery(root PlanNode)
	Children() []ChildPlan
	getFields() []selectTuple
	getReferTables() map[string]*tableInfo
	pushOrderBy(sel sqlparser.SelectStatement) error
	pushLimit(sel sqlparser.SelectStatement) error
	explain() *explain
}

// SelectNode interface.
type SelectNode interface {
	PlanNode
	pushFilter(filters []exprInfo) error
	pushKeyFilter(filter exprInfo, table, field string) error
	setParent(p SelectNode)
	setNoTableFilter(exprs []sqlparser.Expr)
	setParenthese(hasParen bool)
	pushEqualCmpr(joins []exprInfo) SelectNode
	calcRoute() (SelectNode, error)
	pushSelectExprs(fields, groups []selectTuple, sel *sqlparser.Select, aggTyp aggrType) error
	pushSelectExpr(field selectTuple) (int, error)
	pushHaving(havings []exprInfo) error
	pushMisc(sel *sqlparser.Select)
	reOrder(int)
	Order() int
}

// findLCA get the two plannode's lowest common ancestors node.
func findLCA(h, p1, p2 SelectNode) SelectNode {
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

func findParent(tables []string, node SelectNode) SelectNode {
	var parent SelectNode
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

func addFilter(s SelectNode, filter exprInfo) {
	switch node := s.(type) {
	case *JoinNode:
		node.otherFilter = append(node.otherFilter, filter)
	case *MergeNode:
		node.addWhere(filter.expr)
	}
}

// JSON returns the plan info.
func JSON(p PlanNode) string {
	exp := p.explain()
	exp.Project = GetProject(p)
	bout, err := json.MarshalIndent(exp, "", "\t")
	if err != nil {
		return err.Error()
	}
	return common.BytesToString(bout)
}
