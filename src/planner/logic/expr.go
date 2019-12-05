/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package logic

import (
	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
)

type exprInfo struct {
	// filter expr.
	expr sqlparser.Expr
	// referred tables.
	referTables []string
	// colname in the filter expr.
	cols []*sqlparser.ColName
	// val in the filter expr.
	vals sqlparser.ValTuple
}

// conditions record join conditions except the equal conditions in left join.
type conditions struct {
	// filters without tables.
	consts []sqlparser.Expr
	// filters belong to JoinNode.Left.
	lefts []exprInfo
	// filters belong to the JoinNode.Right.
	rights []exprInfo
	// filters cross-shard.
	others []exprInfo
	equals []exprInfo
}

func parseJoinOrWhereExprs(exprs sqlparser.Expr, tbInfos map[string]*tables, other *conditions) error {
	for _, filter := range splitAndExpression(nil, exprs) {
		//filter = convertOrToIn(filter)
		info := exprInfo{
			expr: filter,
		}
		err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			case *sqlparser.ColName:
				info.cols = append(info.cols, node)
				tableName := node.Qualifier.Name.String()
				if tableName == "" {
					if len(tbInfos) == 1 {
						tableName, _ = getOneTableInfo(tbInfos)
					} else {
						return false, errors.Errorf("unsupported: unknown.column.'%s'.in.clause", node.Name.String())
					}
				} else {
					if _, ok := tbInfos[tableName]; !ok {
						return false, errors.Errorf("unsupported: unknown.column.'%s.%s'.in.clause", tableName, node.Name.String())
					}
				}

				if isContainKey(info.referTables, tableName) {
					return true, nil
				}
				info.referTables = append(info.referTables, tableName)
			case *sqlparser.Subquery:
				return false, errors.New("unsupported: subqueries.in.select")
			}
			return true, nil
		}, filter)
		if err != nil {
			return err
		}
		if len(info.cols) == 1 {
			col, vals := fetchColVals(filter)
			if col != nil {
				table := tbInfos[info.referTables[0]]
				if nameMatch(col, table.alias, table.config.ShardKey) {
					info.vals = vals
				}
			}
		}
	}
	return nil
}

func setOtherJoin(infos []exprInfo, left, right map[string]*tables, other *conditions) {
	for _, info := range infos {
		if len(info.cols) == 0 {
			other.consts = append(other.consts, info.expr)
			continue
		}

		fromLeft := true
		fromRight := true
		for _, referTable := range info.referTables {
			if _, ok := left[referTable]; ok {
				fromRight = false
			} else {
				fromLeft = false
			}
		}

		if fromLeft {
			other.lefts = append(other.lefts, info)
		} else if fromRight {
			other.rights = append(other.rights, info)
		} else {
			condition, ok := info.expr.(*sqlparser.ComparisonExpr)
			if ok && condition.Operator == sqlparser.EqualStr {
				_, lok := condition.Left.(*sqlparser.ColName)
				_, rok := condition.Right.(*sqlparser.ColName)
				if lok && rok {
					if _, ok := left[info.referTables[0]]; !ok {
						info.cols[0], info.cols[1] = info.cols[1], info.cols[0]
					}
					other.equals = append(other.equals, info)
				}
			} else {
				other.others = append(other.others, info)
			}
		}
	}
}

func nameMatch(node sqlparser.Expr, table, shardkey string) bool {
	colname, ok := node.(*sqlparser.ColName)
	return ok && (colname.Qualifier.Name.String() == "" || colname.Qualifier.Name.String() == table) && (colname.Name.String() == shardkey)
}

func isContainKey(a []string, b string) bool {
	for _, c := range a {
		if c == b {
			return true
		}
	}
	return false
}

// getOneTableInfo get a tableInfo.
func getOneTableInfo(tbInfos map[string]*tables) (string, *tables) {
	for tb, tbInfo := range tbInfos {
		return tb, tbInfo
	}
	return "", nil
}

// splitAndExpression breaks up the Expr into AND-separated conditions
// and appends them to filters, which can be shuffled and recombined
// as needed.
func splitAndExpression(filters []sqlparser.Expr, node sqlparser.Expr) []sqlparser.Expr {
	if node == nil {
		return filters
	}
	switch node := node.(type) {
	case *sqlparser.AndExpr:
		filters = splitAndExpression(filters, node.Left)
		return splitAndExpression(filters, node.Right)
	case *sqlparser.ParenExpr:
		return splitAndExpression(filters, node.Expr)
	}
	return append(filters, node)
}

// rebuildOr used to rebuild the OrExpr.
func rebuildOr(node, expr sqlparser.Expr) sqlparser.Expr {
	if node == nil {
		return expr
	}
	return &sqlparser.OrExpr{
		Left:  node,
		Right: expr,
	}
}

// TransformOrs transforms the OR expressions. If the cond is OR, try to extract
// the same condition from it and convert the or expression to in expression.
func TransformOrs(exprs []sqlparser.Expr) []sqlparser.Expr {
	var newExprs []sqlparser.Expr
	for _, expr := range exprs {
		or, ok := expr.(*sqlparser.OrExpr)
		if !ok {
			newExprs = append(newExprs, expr)
			continue
		}
		exprMaps, splited, onlyNeedSplited := extractExprsFromDNF(or)
		newExprs = append(newExprs, splited...)
		if !onlyNeedSplited {
			newExprs = append(newExprs, convertOrToIn(exprMaps))
		}
	}
	return newExprs
}

// extractExprsFromDNF extracts the same condition that occurs in every OR args and remove them from OR.
func extractExprsFromDNF(expr *sqlparser.OrExpr) ([]map[string]sqlparser.Expr, []sqlparser.Expr, bool) {
	var subExprs []sqlparser.Expr
	subExprs = splitOrExpression(subExprs, expr)
	exprMaps := make([]map[string]sqlparser.Expr, len(subExprs))
	strCntMap := make(map[string]int)
	strExprMap := make(map[string]sqlparser.Expr)
	for i, expr := range subExprs {
		innerMap := make(map[string]struct{})
		cnfs := splitAndExpression(nil, expr)
		exprs := make(map[string]sqlparser.Expr, len(cnfs))
		for _, cnf := range cnfs {
			buf := sqlparser.NewTrackedBuffer(nil)
			cnf.Format(buf)
			str := buf.String()
			if _, ok := innerMap[str]; ok {
				// Remove the duplicate conditions.
				// eg: `(t1.a=1 and t1.a=1) or ...` is equivalent to the statement `t1.a=1 or ...`.
				continue
			}
			exprs[str] = cnf
			innerMap[str] = struct{}{}
			if i == 0 {
				strCntMap[str] = 1
				strExprMap[str] = cnf
			} else if _, ok := strCntMap[str]; ok {
				strCntMap[str]++
			}
		}
		exprMaps[i] = exprs
	}

	// The expr need exists in every subExprs.
	for str, cnt := range strCntMap {
		if cnt < len(subExprs) {
			delete(strExprMap, str)
		}
	}
	if len(strExprMap) == 0 {
		return exprMaps, nil, false
	}

	onlyNeedSplited := false
	for _, exprs := range exprMaps {
		for str := range strExprMap {
			delete(exprs, str)
		}
		// eg: `(t1.a=1) or (t1.a=1 and t2.a>1)` is equivalent to the statement `t1.a=1`.
		if len(exprs) == 0 {
			onlyNeedSplited = true
			break
		}
	}
	var splited []sqlparser.Expr
	for _, expr := range strExprMap {
		splited = append(splited, expr)
	}
	return exprMaps, splited, onlyNeedSplited
}

// convertOrToIn converts the Expr type from `or` to `in`.
func convertOrToIn(exprMaps []map[string]sqlparser.Expr) sqlparser.Expr {
	var i int
	inMap := make(map[*sqlparser.ColName]sqlparser.ValTuple, len(exprMaps))
	for i < len(exprMaps) {
		match := true
		var col *sqlparser.ColName
		var vals sqlparser.ValTuple
		for _, expr := range exprMaps[i] {
			newCol, newVals := fetchColVals(expr)
			if col == nil {
				col = newCol
			}
			if newCol == nil || !col.Equal(newCol) {
				match = false
				break
			}
			vals = append(vals, newVals...)
		}
		if match {
			col = checkColInMap(inMap, col)
			inMap[col] = append(inMap[col], vals...)
			exprMaps = append(exprMaps[:i], exprMaps[i+1:]...)
			continue
		}
		i++
	}

	var newExpr sqlparser.Expr
	for _, exprs := range exprMaps {
		var sub sqlparser.Expr
		for _, expr := range exprs {
			if sub == nil {
				sub = expr
			}
			sub = &sqlparser.AndExpr{
				Left:  sub,
				Right: expr,
			}
		}
		newExpr = rebuildOr(newExpr, sub)
	}
	for k, v := range inMap {
		sub := &sqlparser.ComparisonExpr{
			Operator: sqlparser.InStr,
			Left:     k,
			Right:    v,
		}
		newExpr = rebuildOr(newExpr, sub)
	}
	return newExpr
}

// checkColInMap used to check if the colname is in the map.
func checkColInMap(inMap map[*sqlparser.ColName]sqlparser.ValTuple, col *sqlparser.ColName) *sqlparser.ColName {
	for k := range inMap {
		if col.Equal(k) {
			return k
		}
	}
	return col
}

// splitOrExpression breaks up the Expr into OR-separated conditions
// and appends them to filters.
func splitOrExpression(filters []sqlparser.Expr, node sqlparser.Expr) []sqlparser.Expr {
	if node == nil {
		return filters
	}
	switch node := node.(type) {
	case *sqlparser.OrExpr:
		filters = splitOrExpression(filters, node.Left)
		return splitOrExpression(filters, node.Right)
	case *sqlparser.ParenExpr:
		return splitOrExpression(filters, node.Expr)
	}
	return append(filters, node)
}

// fetchColVals fetch ColName and ValTuple from Expr which type is `in` or `=`.
func fetchColVals(node sqlparser.Expr) (*sqlparser.ColName, sqlparser.ValTuple) {
	var col *sqlparser.ColName
	var vals sqlparser.ValTuple
	if expr, ok := node.(*sqlparser.ComparisonExpr); ok {
		switch expr.Operator {
		case sqlparser.EqualStr:
			if _, ok := expr.Left.(*sqlparser.SQLVal); ok {
				expr.Left, expr.Right = expr.Right, expr.Left
			}
			if lc, ok := expr.Left.(*sqlparser.ColName); ok {
				if val, ok := expr.Right.(*sqlparser.SQLVal); ok {
					col = lc
					vals = append(vals, val)
				}
			}
		case sqlparser.InStr:
			if lc, ok := expr.Left.(*sqlparser.ColName); ok {
				if valTuple, ok := expr.Right.(sqlparser.ValTuple); ok {
					col = lc
					vals = valTuple
				}
			}
		}
	}
	return col, vals
}
