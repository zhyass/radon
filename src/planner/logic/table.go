/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package logic

import (
	"config"
	"router"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// tableInfo represents one table information.
type tables struct {
	// database.
	database string
	// table's name.
	name string
	// table's alias.
	alias string
	// table's config.
	config *config.TableConfig
	// table expression in select ast 'From'.
	expr *sqlparser.AliasedTableExpr
	// table's route.
	segs []router.Segment
	// table's parent node.
	node node
}

// from 里面返回了所有的tables.
// 我们需要判断其他的字段是否合法.需要使用这些tables.
// 先生成一个map类型。
// 最终路由的定型是在
// 再调用optimizer进行优化。
func procesTableExprs(log *xlog.Log, router *router.Router, database string, tableExprs sqlparser.TableExprs) (node, error) {
	if len(tableExprs) == 1 {
		return procesTableExpr(log, router, database, tableExprs[0])
	}

	lpn, err := procesTableExpr(log, router, database, tableExprs[0])
	if err != nil {
		return nil, err
	}

	rpn, err := procesTableExprs(log, router, database, tableExprs[1:])
	if err != nil {
		return nil, err
	}
	return joinner(log, lpn, rpn, nil, router)
}

func procesTableExpr(log *xlog.Log, router *router.Router, database string, tableExpr sqlparser.TableExpr) (node, error) {
	var err error
	var p node
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		p, err = procesAliasedTableExpr(log, router, database, tableExpr)
	case *sqlparser.JoinTableExpr:
		p, err = procesJoinTableExpr(log, router, database, tableExpr)
	case *sqlparser.ParenTableExpr:
		p, err = procesTableExprs(log, router, database, tableExpr.Exprs)
		// If finally p is a MergeNode, the pushed query need keep the parenthese.
		//setParenthese(p, true)
	}
	return p, err
}

// procesAliasedTableExpr produces the table's tableInfo by the AliasedTableExpr, and build a MergeNode subtree.
func procesAliasedTableExpr(log *xlog.Log, r *router.Router, database string, tableExpr *sqlparser.AliasedTableExpr) (node, error) {
	var err error
	mn := newRoute()
	switch expr := tableExpr.Expr.(type) {
	case sqlparser.TableName:
		if expr.Qualifier.IsEmpty() {
			expr.Qualifier = sqlparser.NewTableIdent(database)
		}
		tn := &tables{
			database: expr.Qualifier.String(),
			name:     expr.Name.String(),
			expr:     tableExpr,
			alias:    tableExpr.As.String(),
			node:     mn,
		}
		if tn.config, err = r.TableConfig(tn.database, tn.name); err != nil {
			return nil, err
		}
		if tn.alias == "" {
			tn.alias = tn.name
		}

		switch tn.config.ShardType {
		case "GLOBAL":
			mn.nonGlobalCnt = 0
		case "SINGLE":
			mn.indexes = append(mn.indexes, 0)
			mn.nonGlobalCnt = 1
		case "HASH", "LIST":
			// if a shard table hasn't alias, create one in order to push.
			if tableExpr.As.IsEmpty() {
				tableExpr.As = sqlparser.NewTableIdent(tn.name)
			}
			mn.nonGlobalCnt = 1
		}
	case *sqlparser.Subquery:
		err = errors.New("unsupported: subquery.in.select")
	}
	mn.Select = &sqlparser.Select{From: sqlparser.TableExprs([]sqlparser.TableExpr{tableExpr})}
	return mn, err
}

func procesJoinTableExpr(log *xlog.Log, router *router.Router, database string, joinExpr *sqlparser.JoinTableExpr) (node, error) {
	switch joinExpr.Join {
	case sqlparser.JoinStr, sqlparser.StraightJoinStr, sqlparser.LeftJoinStr:
	case sqlparser.RightJoinStr:
		convertToLeftJoin(joinExpr)
	default:
		return nil, errors.Errorf("unsupported: join.type:%s", joinExpr.Join)
	}

	lpn, err := procesTableExpr(log, router, database, joinExpr.LeftExpr)
	if err != nil {
		return nil, err
	}

	rpn, err := procesTableExpr(log, router, database, joinExpr.RightExpr)
	if err != nil {
		return nil, err
	}
	return joinner(log, lpn, rpn, joinExpr, router)
}

// join 和left join分开处理？
func joinner(log *xlog.Log, lpn, rpn node, joinExpr *sqlparser.JoinTableExpr, router *router.Router) (node, error) {
	
	return nil, nil
}

// convertToLeftJoin converts a right join into a left join.
func convertToLeftJoin(joinExpr *sqlparser.JoinTableExpr) {
	newExpr := joinExpr.LeftExpr
	// If LeftExpr is a join, we have to parenthesize it.
	if _, ok := newExpr.(*sqlparser.JoinTableExpr); ok {
		newExpr = &sqlparser.ParenTableExpr{
			Exprs: sqlparser.TableExprs{newExpr},
		}
	}
	joinExpr.LeftExpr, joinExpr.RightExpr = joinExpr.RightExpr, newExpr
	joinExpr.Join = sqlparser.LeftJoinStr
}
