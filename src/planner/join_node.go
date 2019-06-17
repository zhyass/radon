/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"fmt"
	"router"
	"xcontext"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// JoinStrategy is Join Strategy.
type JoinStrategy int

const (
	// Cartesian product.
	Cartesian JoinStrategy = iota
	// SortMerge Join.
	SortMerge
	// NestedLoop Join.
	NestedLoop
)

// JoinKey is the column info in the on conditions.
type JoinKey struct {
	// field name.
	Field string
	// table name.
	Table string
	// index in the fields.
	Index int
}

// Comparison is record the sqlparser.Comparison info.
type Comparison struct {
	// index in left and right node's fields.
	Left, Right int
	Operator    string
	// left expr may in right node.
	Exchange bool
}

// JoinNode cannot be pushed down.
type JoinNode struct {
	log *xlog.Log
	// router.
	router *router.Router
	// Left and Right are the nodes for the join.
	Left, Right PlanNode
	// join strategy.
	Strategy JoinStrategy
	// JoinTableExpr in FROM clause.
	joinExpr *sqlparser.JoinTableExpr
	// referred tables' tableInfo map.
	referredTables map[string]*TableInfo
	// whether has parenthese in FROM clause.
	hasParen bool
	// parent node in the plan tree.
	parent PlanNode
	// children plans in select(such as: orderby, limit..).
	children *PlanTree
	// Cols defines which columns from left or right results used to build the return result.
	// For results coming from left, the values go as -1, -2, etc. For right, they're 1, 2, etc.
	// If Cols is {-1, -2, 1, 2}, it means the returned result is {Left0, Left1, Right0, Right1}.
	Cols []int `json:",omitempty"`
	// the returned result fields.
	fields []selectTuple
	// join on condition tuples.
	joinOn []joinTuple
	// eg: from t1 join t2 on t1.a=t2.b, 't1.a' put in LeftKeys, 't2.a' in RightKeys.
	LeftKeys, RightKeys []JoinKey
	// eg: t1 join t2 on t1.a>t2.a, 't1.a>t2.a' parser into CmpFilter.
	CmpFilter []Comparison
	/*
	 * eg: 't1 left join t2 on t1.a=t2.a and t1.b=2' where t1.c=t2.c and 1=1 and t2.b>2 where
	 * t2.str is null. 't1.b=2' will parser into otherJoinOn, IsLeftJoin is true, 't1.c=t2.c'
	 * parser into otherFilter, else into joinOn. '1=1' parser into noTableFilter. 't2.b>2' into
	 * tableFilter. 't2.str is null' into rightNull.
	 */
	tableFilter, otherFilter []filterTuple
	noTableFilter            []sqlparser.Expr
	otherJoinOn              *otherJoin
	rightNull                []nullExpr
	// whether is left join.
	IsLeftJoin bool
	// whether the right node has filters in left join.
	HasRightFilter bool
	// record the `otherJoin.left`'s index in left.fields.
	LeftTmpCols []int
	// record the `rightNull`'s index in right.fields.
	RightTmpCols []int
	// keyFilters based on LeftKeys、RightKeys and tableFilter.
	// eg: select * from t1 join t2 on t1.a=t2.a where t1.a=1
	// `t1.a` in LeftKeys, `t1.a=1` in tableFilter. in the map,
	// key is 0(index is 0), value is tableFilter(`t1.a=1`).
	keyFilters map[int][]filterTuple
	// isHint defines whether has /*+nested+*/.
	isHint bool
	order  int
	// Vars defines the list of joinVars that need to be built
	// from the Left result before invoking the Right subqquery.
	Vars map[string]int
}

// newJoinNode used to create JoinNode.
func newJoinNode(log *xlog.Log, Left, Right PlanNode, router *router.Router, joinExpr *sqlparser.JoinTableExpr,
	joinOn []joinTuple, referredTables map[string]*TableInfo) *JoinNode {
	isLeftJoin := false
	if joinExpr != nil && joinExpr.Join == sqlparser.LeftJoinStr {
		isLeftJoin = true
	}
	return &JoinNode{
		log:            log,
		Left:           Left,
		Right:          Right,
		router:         router,
		joinExpr:       joinExpr,
		joinOn:         joinOn,
		keyFilters:     make(map[int][]filterTuple),
		Vars:           make(map[string]int),
		referredTables: referredTables,
		IsLeftJoin:     isLeftJoin,
		children:       NewPlanTree(),
	}
}

// getReferredTables get the referredTables.
func (j *JoinNode) getReferredTables() map[string]*TableInfo {
	return j.referredTables
}

// getFields get the fields.
func (j *JoinNode) getFields() []selectTuple {
	return j.fields
}

// setParenthese set hasParen.
func (j *JoinNode) setParenthese(hasParen bool) {
	j.hasParen = hasParen
}

// pushFilter used to push the filters.
func (j *JoinNode) pushFilter(filters []filterTuple) error {
	var err error
	rightTbs := j.Right.getReferredTables()
	for _, filter := range filters {
		if len(filter.referTables) == 0 {
			j.noTableFilter = append(j.noTableFilter, filter.expr)
			continue
		}
		// if left join's right node is null condition will not be pushed down.
		if j.IsLeftJoin {
			if ok, nullFunc := checkIsWithNull(filter, rightTbs); ok {
				j.rightNull = append(j.rightNull, nullFunc)
				continue
			}
		}
		if len(filter.referTables) == 1 {
			tb := filter.referTables[0]
			tbInfo := j.referredTables[tb]
			if filter.col == nil {
				tbInfo.parent.setWhereFilter(filter)
			} else {
				j.tableFilter = append(j.tableFilter, filter)
				if len(filter.vals) > 0 && tbInfo.shardKey != "" {
					if nameMatch(filter.col, tb, tbInfo.shardKey) {
						for _, val := range filter.vals {
							if err = getIndex(j.router, tbInfo, val); err != nil {
								return err
							}
						}
					}
				}
			}
		} else {
			var parent PlanNode
			for _, tb := range filter.referTables {
				tbInfo := j.referredTables[tb]
				if parent == nil {
					parent = tbInfo.parent
					continue
				}
				if parent != tbInfo.parent {
					parent = findLCA(j, parent, tbInfo.parent)
				}
			}
			parent.setWhereFilter(filter)
		}
		if j.IsLeftJoin && !j.HasRightFilter {
			for _, tb := range filter.referTables {
				if _, ok := rightTbs[tb]; ok {
					j.HasRightFilter = true
					break
				}
			}
		}
	}
	return err
}

// setParent set the parent node.
func (j *JoinNode) setParent(p PlanNode) {
	j.parent = p
}

// setWhereFilter set the otherFilter.
func (j *JoinNode) setWhereFilter(filter filterTuple) {
	j.otherFilter = append(j.otherFilter, filter)
}

// setNoTableFilter used to push the no table filters.
func (j *JoinNode) setNoTableFilter(exprs []sqlparser.Expr) {
	j.noTableFilter = append(j.noTableFilter, exprs...)
}

// otherJoin is the filter in leftjoin's on clause.
// based on the plan tree,separate the otherjoinon.
type otherJoin struct {
	// noTables: no tables filter in otherjoinon.
	noTables []sqlparser.Expr
	// filter belong to the left node.
	left []selectTuple
	// filter belong to the right node.
	// others: filter cross the left and right.
	right, others []filterTuple
}

// setOtherJoin use to process the otherjoinon.
func (j *JoinNode) setOtherJoin(filters []filterTuple) {
	j.otherJoinOn = &otherJoin{}
	i := 0
	for _, filter := range filters {
		if len(filter.referTables) == 0 {
			j.otherJoinOn.noTables = append(j.otherJoinOn.noTables, filter.expr)
			continue
		}
		if checkTbInNode(filter.referTables, j.Left.getReferredTables()) {
			buf := sqlparser.NewTrackedBuffer(nil)
			filter.expr.Format(buf)
			field := buf.String()

			alias := fmt.Sprintf("tmpc_%d", i)
			tuple := selectTuple{
				expr:        &sqlparser.AliasedExpr{Expr: filter.expr, As: sqlparser.NewColIdent(alias)},
				field:       field,
				alias:       alias,
				referTables: filter.referTables,
			}
			j.otherJoinOn.left = append(j.otherJoinOn.left, tuple)
			i++
		} else if checkTbInNode(filter.referTables, j.Right.getReferredTables()) {
			j.otherJoinOn.right = append(j.otherJoinOn.right, filter)
		} else {
			j.otherJoinOn.others = append(j.otherJoinOn.others, filter)
		}
	}
}

// pushOtherJoin use to push otherjoin.
// eg: select A.a from A left join B on A.id=B.id and 1=1 and A.c=1 and B.b='a';
// push: select A.c=1 as tmpc_0,A.a,A.id from A order by A.id asc;
//       select B.id from B where 1=1 and B.b='a' order by B.id asc;
func (j *JoinNode) pushOtherJoin(idx *int) error {
	if j.otherJoinOn != nil {
		if len(j.otherJoinOn.others) > 0 {
			if err := j.pushOtherFilters(j.otherJoinOn.others, idx, true); err != nil {
				return err
			}
		}
		if len(j.otherJoinOn.noTables) > 0 {
			j.Right.setNoTableFilter(j.otherJoinOn.noTables)
		}
		if len(j.otherJoinOn.left) > 0 {
			for _, field := range j.otherJoinOn.left {
				index, err := j.Left.pushSelectExpr(field)
				if err != nil {
					return err
				}
				j.LeftTmpCols = append(j.LeftTmpCols, index)
			}
		}
		if len(j.otherJoinOn.right) > 0 {
			for _, filter := range j.otherJoinOn.right {
				var parent PlanNode
				for _, tb := range filter.referTables {
					tbInfo := j.referredTables[tb]
					if parent == nil {
						parent = tbInfo.parent
						continue
					}
					if parent != tbInfo.parent {
						if j.isHint {
							if parent.Order() < tbInfo.parent.Order() {
								parent = tbInfo.parent
							}
						} else {
							parent = findLCA(j.Right, parent, tbInfo.parent)
						}
					}
				}

				if mn, ok := parent.(*MergeNode); ok {
					mn.setWhereFilter(filter)
				} else {
					buf := sqlparser.NewTrackedBuffer(nil)
					filter.expr.Format(buf)
					return errors.Errorf("unsupported: on.clause.'%s'.in.cross-shard.join", buf.String())
				}
			}
		}
	}
	return nil
}

// pushEqualCmpr used to push the equal Comparison type filters.
// eg: 'select * from t1, t2 where t1.a=t2.a and t1.b=2'.
// 't1.a=t2.a' is the 'join' type filters.
func (j *JoinNode) pushEqualCmpr(joins []joinTuple) PlanNode {
	for i, joinFilter := range joins {
		var parent PlanNode
		ltb := j.referredTables[joinFilter.referTables[0]]
		rtb := j.referredTables[joinFilter.referTables[1]]
		parent = findLCA(j, ltb.parent, rtb.parent)

		switch node := parent.(type) {
		case *MergeNode:
			node.Sel.AddWhere(joinFilter.expr)
		case *JoinNode:
			join, _ := checkJoinOn(node.Left, node.Right, joinFilter)
			if lmn, ok := node.Left.(*MergeNode); ok {
				if rmn, ok := node.Right.(*MergeNode); ok {
					if isSameShard(lmn.referredTables, rmn.referredTables, join.left, join.right) {
						mn, _ := mergeRoutes(lmn, rmn, node.joinExpr, nil)
						mn.setParent(node.parent)
						mn.setParenthese(node.hasParen)

						for _, filter := range node.tableFilter {
							mn.setWhereFilter(filter)
						}
						for _, filter := range node.otherFilter {
							mn.setWhereFilter(filter)
						}
						for _, exprs := range node.noTableFilter {
							mn.Sel.AddWhere(exprs)
						}

						if node.joinExpr == nil {
							for _, joins := range node.joinOn {
								mn.Sel.AddWhere(joins.expr)
							}
						}
						mn.Sel.AddWhere(join.expr)
						if node.parent == nil {
							return mn.pushEqualCmpr(joins[i+1:])
						}

						j := node.parent.(*JoinNode)
						if j.Left == node {
							j.Left = mn
						} else {
							j.Right = mn
						}
						continue
					}
				}
			}
			if node.IsLeftJoin {
				node.setWhereFilter(filterTuple{expr: join.expr, referTables: join.referTables})
			} else {
				node.joinOn = append(node.joinOn, join)
				if node.joinExpr != nil {
					node.joinExpr.On = &sqlparser.AndExpr{
						Left:  node.joinExpr.On,
						Right: join.expr,
					}
				}
			}
		}
	}
	return j
}

// calcRoute used to calc the route.
func (j *JoinNode) calcRoute() (PlanNode, error) {
	var err error
	for _, filter := range j.tableFilter {
		if !j.buildKeyFilter(filter, false) {
			tbInfo := j.referredTables[filter.referTables[0]]
			tbInfo.parent.setWhereFilter(filter)
		}
	}
	if j.Left, err = j.Left.calcRoute(); err != nil {
		return j, err
	}
	if j.Right, err = j.Right.calcRoute(); err != nil {
		return j, err
	}

	// left and right node have same routes.
	if lmn, ok := j.Left.(*MergeNode); ok {
		if rmn, ok := j.Right.(*MergeNode); ok {
			if (lmn.backend != "" && lmn.backend == rmn.backend) || rmn.nonGlobalCnt == 0 || lmn.nonGlobalCnt == 0 {
				if lmn.nonGlobalCnt == 0 {
					lmn.backend = rmn.backend
					lmn.routeLen = rmn.routeLen
					lmn.index = rmn.index
				}
				mn, _ := mergeRoutes(lmn, rmn, j.joinExpr, nil)
				mn.setParent(j.parent)
				mn.setParenthese(j.hasParen)
				for _, filter := range j.otherFilter {
					mn.setWhereFilter(filter)
				}
				for _, filters := range j.keyFilters {
					for _, filter := range filters {
						mn.setWhereFilter(filter)
					}
				}
				for _, exprs := range j.noTableFilter {
					mn.Sel.AddWhere(exprs)
				}
				if j.joinExpr == nil && len(j.joinOn) > 0 {
					for _, joins := range j.joinOn {
						mn.Sel.AddWhere(joins.expr)
					}
				}
				return mn, nil
			}
		}
	}

	return j, nil
}

// buildKeyFilter used to build the keyFilter based on the tableFilter and joinOn.
// eg: select t1.a,t2.a from t1 join t2 on t1.a=t2.a where t1.a=1;
// push: select t1.a from t1 where t1.a=1 order by t1.a asc;
//       select t2.a from t2 where t2.a=1 order by t2.a asc;
func (j *JoinNode) buildKeyFilter(filter filterTuple, isFind bool) bool {
	table := filter.col.Qualifier.Name.String()
	field := filter.col.Name.String()
	find := false
	if _, ok := j.Left.getReferredTables()[filter.referTables[0]]; ok {
		for i, join := range j.joinOn {
			lt := join.left.Qualifier.Name.String()
			lc := join.left.Name.String()
			if lt == table && lc == field {
				j.keyFilters[i] = append(j.keyFilters[i], filter)
				if len(filter.vals) > 0 {
					rt := join.right.Qualifier.Name.String()
					rc := join.right.Name.String()
					tbInfo := j.referredTables[rt]
					if tbInfo.shardKey == rc {
						for _, val := range filter.vals {
							if err := getIndex(j.router, tbInfo, val); err != nil {
								panic(err)
							}
						}
					}
				}
				find = true
				break
			}
		}
		if jn, ok := j.Left.(*JoinNode); ok {
			return jn.buildKeyFilter(filter, find || isFind)
		}
	} else {
		for i, join := range j.joinOn {
			rt := join.right.Qualifier.Name.String()
			rc := join.right.Name.String()
			if rt == table && rc == field {
				j.keyFilters[i] = append(j.keyFilters[i], filter)
				if len(filter.vals) > 0 {
					lt := join.left.Qualifier.Name.String()
					lc := join.left.Name.String()
					tbInfo := j.referredTables[lt]
					if tbInfo.shardKey == lc {
						for _, val := range filter.vals {
							if err := getIndex(j.router, tbInfo, val); err != nil {
								panic(err)
							}
						}
					}
				}
				find = true
				break
			}
		}
		if jn, ok := j.Right.(*JoinNode); ok {
			return jn.buildKeyFilter(filter, find || isFind)
		}
	}
	return find || isFind
}

// pushSelectExprs used to push the select fields.
func (j *JoinNode) pushSelectExprs(fields, groups []selectTuple, sel *sqlparser.Select, aggTyp aggrType) error {
	if j.isHint {
		j.reOrder(0)
	}

	if len(groups) > 0 || aggTyp != nullAgg {
		aggrPlan := NewAggregatePlan(j.log, sel.SelectExprs, fields, groups, false)
		if err := aggrPlan.Build(); err != nil {
			return err
		}
		j.children.Add(aggrPlan)
		fields = aggrPlan.tuples
	}
	for _, tuple := range fields {
		if _, err := j.pushSelectExpr(tuple); err != nil {
			return err
		}
	}
	j.handleJoinOn()

	return j.handleOthers()
}

// handleOthers used to handle otherJoinOn|rightNull|otherFilter.
func (j *JoinNode) handleOthers() error {
	var err error
	var idx int
	if lp, ok := j.Left.(*JoinNode); ok {
		if err = lp.handleOthers(); err != nil {
			return err
		}
	}

	if rp, ok := j.Right.(*JoinNode); ok {
		if err = rp.handleOthers(); err != nil {
			return err
		}
	}

	if err = j.pushOtherJoin(&idx); err != nil {
		return err
	}

	if err = j.pushNullExprs(&idx); err != nil {
		return err
	}

	return j.pushOtherFilters(j.otherFilter, &idx, false)
}

// pushNullExprs used to push rightNull.
func (j *JoinNode) pushNullExprs(idx *int) error {
	for _, tuple := range j.rightNull {
		index, err := j.pushOtherFilter(tuple.expr, j.Right, tuple.referTables, idx)
		if err != nil {
			return err
		}
		j.RightTmpCols = append(j.RightTmpCols, index)
	}
	return nil
}

// pushOtherFilters used to push otherFilter.
func (j *JoinNode) pushOtherFilters(filters []filterTuple, idx *int, isOtherJoin bool) error {
	for _, filter := range filters {
		var err error
		var lidx, ridx int
		var exchange bool
		if j.isHint {
			var m *MergeNode
			for _, tb := range filter.referTables {
				tbInfo := j.referredTables[tb]
				if m == nil {
					m = tbInfo.parent
					continue
				}
				if m.Order() < tbInfo.parent.Order() {
					m = tbInfo.parent
				}
			}
			m.setWhereFilter(filter)
			continue
		}
		if exp, ok := filter.expr.(*sqlparser.ComparisonExpr); ok {
			left := getTbInExpr(exp.Left)
			right := getTbInExpr(exp.Right)
			ltb := j.Left.getReferredTables()
			rtb := j.Right.getReferredTables()
			if exp.Operator == sqlparser.EqualStr && (isOtherJoin || !j.IsLeftJoin) &&
				len(left) == 1 && len(right) == 1 {
				if !checkTbInNode(left, ltb) {
					exp.Left, exp.Right = exp.Right, exp.Left
					left, right = right, left
				}
				leftKey := j.buildOrderBy(exp.Left, j.Left, idx)
				rightKey := j.buildOrderBy(exp.Right, j.Right, idx)
				j.LeftKeys = append(j.LeftKeys, leftKey)
				j.RightKeys = append(j.RightKeys, rightKey)
				continue
			}
			if checkTbInNode(left, ltb) && checkTbInNode(right, rtb) {
				if lidx, err = j.pushOtherFilter(exp.Left, j.Left, left, idx); err != nil {
					return err
				}
				if ridx, err = j.pushOtherFilter(exp.Right, j.Right, right, idx); err != nil {
					return err
				}
			} else if checkTbInNode(left, rtb) && checkTbInNode(right, ltb) {
				if lidx, err = j.pushOtherFilter(exp.Right, j.Left, right, idx); err != nil {
					return err
				}
				if ridx, err = j.pushOtherFilter(exp.Left, j.Right, left, idx); err != nil {
					return err
				}
				exchange = true
			} else {
				buf := sqlparser.NewTrackedBuffer(nil)
				exp.Format(buf)
				return errors.Errorf("unsupported: clause.'%s'.in.cross-shard.join", buf.String())
			}
			j.CmpFilter = append(j.CmpFilter, Comparison{lidx, ridx, exp.Operator, exchange})
		} else {
			buf := sqlparser.NewTrackedBuffer(nil)
			filter.expr.Format(buf)
			return errors.Errorf("unsupported: clause.'%s'.in.cross-shard.join", buf.String())
		}

		err = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			case *sqlparser.ColName:
				err := j.pushColName(node)
				if err != nil {
					return false, err
				}
			}
			return true, nil
		}, filter.expr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (j *JoinNode) pushColName(col *sqlparser.ColName) error {
	var err error
	index := -1
	table := col.Qualifier.Name.String()
	field := col.Name.String()
	node := j.referredTables[table].parent
	tuples := node.getFields()
	for i, tuple := range tuples {
		if tuple.isCol {
			if table == tuple.referTables[0] && field == tuple.field {
				index = i
				break
			}
		}
	}

	// key not in the select fields.
	if index == -1 {
		aliasExpr := &sqlparser.AliasedExpr{Expr: col}
		tuple := selectTuple{
			expr:        aliasExpr,
			field:       field,
			referTables: []string{table},
			isCol:       true,
		}
		index, err = node.pushSelectExpr(tuple)
		if err != nil {
			return err
		}
	}

	col.Metadata = &sqlparser.Column{Index: index}
	return nil
}

// pushOtherFilter used to push otherFilter.
func (j *JoinNode) pushOtherFilter(expr sqlparser.Expr, node PlanNode, tbs []string, idx *int) (int, error) {
	var err error
	var field, alias string
	index := -1
	if col, ok := expr.(*sqlparser.ColName); ok {
		field = col.Name.String()
		table := col.Qualifier.Name.String()
		tuples := node.getFields()
		for i, tuple := range tuples {
			if tuple.isCol {
				if table == tuple.referTables[0] && field == tuple.field {
					index = i
					break
				}
			}
		}
	}
	// key not in the select fields.
	if index == -1 {
		aliasExpr := &sqlparser.AliasedExpr{Expr: expr}
		isCol := true
		if field == "" {
			isCol = false
			buf := sqlparser.NewTrackedBuffer(nil)
			expr.Format(buf)
			field = buf.String()

			alias = fmt.Sprintf("tmpo_%d", *idx)
			as := sqlparser.NewColIdent(alias)
			aliasExpr.As = as
			(*idx)++
		}

		tuple := selectTuple{
			expr:        aliasExpr,
			field:       field,
			alias:       alias,
			referTables: tbs,
			isCol:       isCol,
		}
		index, err = node.pushSelectExpr(tuple)
		if err != nil {
			return index, err
		}
	}

	return index, nil
}

// pushSelectExpr used to push the select field.
func (j *JoinNode) pushSelectExpr(field selectTuple) (int, error) {
	if checkTbInNode(field.referTables, j.Left.getReferredTables()) {
		index, err := j.Left.pushSelectExpr(field)
		if err != nil {
			return -1, err
		}
		j.Cols = append(j.Cols, -index-1)
	} else {
		if exp, ok := field.expr.(*sqlparser.AliasedExpr); ok && j.IsLeftJoin {
			if _, ok := exp.Expr.(*sqlparser.FuncExpr); ok {
				return -1, errors.Errorf("unsupported: expr.'%s'.in.cross-shard.left.join", field.field)
			}
		}
		if checkTbInNode(field.referTables, j.Right.getReferredTables()) || j.isHint {
			index, err := j.Right.pushSelectExpr(field)
			if err != nil {
				return -1, err
			}
			j.Cols = append(j.Cols, index+1)
		} else {
			return -1, errors.Errorf("unsupported: expr.'%s'.in.cross-shard.join", field.field)
		}
	}
	j.fields = append(j.fields, field)
	return len(j.fields) - 1, nil
}

// handleJoinOn used to build order by based on On conditions.
func (j *JoinNode) handleJoinOn() {
	// eg: select t1.a,t2.a from t1 join t2 on t1.a=t2.a;
	// push: select t1.a from t1 order by t1.a asc;
	//       select t2.a from t2 order by t2.a asc;
	_, lok := j.Left.(*MergeNode)
	if !lok {
		j.Left.(*JoinNode).handleJoinOn()
	}

	_, rok := j.Right.(*MergeNode)
	if !rok {
		j.Right.(*JoinNode).handleJoinOn()
	}

	for _, join := range j.joinOn {
		var leftKey, rightKey JoinKey
		if j.isHint {
			lt := join.left.Qualifier.Name.String()
			rt := join.right.Qualifier.Name.String()
			ltb := j.referredTables[lt]
			rtb := j.referredTables[rt]
			m := ltb.parent
			if m.Order() < rtb.parent.Order() {
				m = rtb.parent
			}
			m.Sel.AddWhere(join.expr)
			leftKey = JoinKey{Field: join.left.Name.String(),
				Table: lt,
			}
			rightKey = JoinKey{Field: join.right.Name.String(),
				Table: rt,
			}
		} else {
			leftKey = j.buildOrderBy(join.left, j.Left, nil)
			rightKey = j.buildOrderBy(join.right, j.Right, nil)
		}
		j.LeftKeys = append(j.LeftKeys, leftKey)
		j.RightKeys = append(j.RightKeys, rightKey)
	}
}

func (j *JoinNode) buildOrderBy(expr sqlparser.Expr, node PlanNode, idx *int) JoinKey {
	var field, table, alias string
	var col *sqlparser.ColName
	index := -1
	switch exp := expr.(type) {
	case *sqlparser.ColName:
		tuples := node.getFields()
		field = exp.Name.String()
		table = exp.Qualifier.Name.String()
		for i, tuple := range tuples {
			if tuple.isCol {
				if table == tuple.referTables[0] && field == tuple.field {
					index = i
					break
				}
			}
		}
		col = exp
	}

	// key not in the select fields.
	if index == -1 {
		aliasExpr := &sqlparser.AliasedExpr{Expr: expr}
		if field == "" {
			buf := sqlparser.NewTrackedBuffer(nil)
			expr.Format(buf)
			field = buf.String()

			alias = fmt.Sprintf("tmpo_%d", *idx)
			as := sqlparser.NewColIdent(alias)
			aliasExpr.As = as
			col = &sqlparser.ColName{Name: as}
			(*idx)++
		}
		tuple := selectTuple{
			expr:        aliasExpr,
			field:       field,
			alias:       alias,
			referTables: []string{table},
		}
		index, _ = node.pushSelectExpr(tuple)
	}

	if m, ok := node.(*MergeNode); ok {
		m.Sel.OrderBy = append(m.Sel.OrderBy, &sqlparser.Order{
			Expr:      col,
			Direction: sqlparser.AscScr,
		})
	}

	return JoinKey{field, table, index}
}

// pushHaving used to push having exprs.
func (j *JoinNode) pushHaving(havings []filterTuple) error {
	for _, filter := range havings {
		if len(filter.referTables) == 0 {
			j.Left.pushHaving([]filterTuple{filter})
			j.Right.pushHaving([]filterTuple{filter})
		} else if len(filter.referTables) == 1 {
			tbInfo := j.referredTables[filter.referTables[0]]
			tbInfo.parent.Sel.AddHaving(filter.expr)
		} else {
			var parent PlanNode
			for _, tb := range filter.referTables {
				tbInfo := j.referredTables[tb]
				if parent == nil {
					parent = tbInfo.parent
					continue
				}
				if parent != tbInfo.parent {
					if j.isHint {
						if parent.Order() < tbInfo.parent.Order() {
							parent = tbInfo.parent
						}
					} else {
						parent = findLCA(j, parent, tbInfo.parent)
					}
				}
			}
			if mn, ok := parent.(*MergeNode); ok {
				mn.Sel.AddHaving(filter.expr)
			} else {
				buf := sqlparser.NewTrackedBuffer(nil)
				filter.expr.Format(buf)
				return errors.Errorf("unsupported: havings.'%s'.in.cross-shard.join", buf.String())
			}
		}
	}
	return nil
}

// pushOrderBy used to push the order by exprs.
func (j *JoinNode) pushOrderBy(sel *sqlparser.Select, fields []selectTuple) error {
	if len(sel.OrderBy) == 0 {
		for _, by := range sel.GroupBy {
			sel.OrderBy = append(sel.OrderBy, &sqlparser.Order{
				Expr:      by,
				Direction: sqlparser.AscScr,
			})
		}
	}

	if len(sel.OrderBy) > 0 {
		orderPlan := NewOrderByPlan(j.log, sel, fields, j.referredTables)
		if err := orderPlan.Build(); err != nil {
			return err
		}
		j.children.Add(orderPlan)
	}

	return nil
}

// pushLimit used to push limit.
func (j *JoinNode) pushLimit(sel *sqlparser.Select) error {
	limitPlan := NewLimitPlan(j.log, sel)
	if err := limitPlan.Build(); err != nil {
		return err
	}
	j.children.Add(limitPlan)
	return nil
}

// pushMisc used tp push miscelleaneous constructs.
func (j *JoinNode) pushMisc(sel *sqlparser.Select) {
	if len(sel.Comments) > 0 {
		hint := "/*+nested+*/"
		if common.BytesToString(sel.Comments[0]) == hint {
			j.isHint = true
		}
	}
	j.Left.pushMisc(sel)
	j.Right.pushMisc(sel)
}

// Children returns the children of the plan.
func (j *JoinNode) Children() *PlanTree {
	return j.children
}

// reOrder satisfies the plannode interface.
func (j *JoinNode) reOrder(order int) {
	j.Left.reOrder(order)
	j.Right.reOrder(j.Left.Order())
	j.order = j.Right.Order() + 1
}

// Order satisfies the plannode interface.
func (j *JoinNode) Order() int {
	return j.order
}

// buildQuery used to build the QueryTuple.
func (j *JoinNode) buildQuery(tbInfos map[string]*TableInfo) {
	if j.isHint {
		j.Strategy = NestedLoop
	} else {
		if len(j.LeftKeys) == 0 && len(j.CmpFilter) == 0 {
			j.Strategy = Cartesian
		} else {
			j.Strategy = SortMerge
		}
	}

	for i, filters := range j.keyFilters {
		table := j.RightKeys[i].Table
		field := j.RightKeys[i].Field
		tbInfo := j.referredTables[table]
		for _, filter := range filters {
			filter.col.Qualifier.Name = sqlparser.NewTableIdent(table)
			filter.col.Name = sqlparser.NewColIdent(field)
			tbInfo.parent.filters[filter.expr] = 0
		}
	}
	j.Right.setNoTableFilter(j.noTableFilter)
	j.Right.buildQuery(tbInfos)

	for i, filters := range j.keyFilters {
		table := j.LeftKeys[i].Table
		field := j.LeftKeys[i].Field
		tbInfo := j.referredTables[table]
		for _, filter := range filters {
			filter.col.Qualifier.Name = sqlparser.NewTableIdent(table)
			filter.col.Name = sqlparser.NewColIdent(field)
			tbInfo.parent.filters[filter.expr] = 0
		}
	}
	j.Left.setNoTableFilter(j.noTableFilter)
	j.Left.buildQuery(tbInfos)
}

// GetQuery used to get the Querys.
func (j *JoinNode) GetQuery() []xcontext.QueryTuple {
	querys := j.Left.GetQuery()
	querys = append(querys, j.Right.GetQuery()...)
	return querys
}
