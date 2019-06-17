/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package sqlparser

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

const (
	NotFixedDec     = 31
	DecimalMaxScale = 30
)

type ResultType int

const (
	StringResult ResultType = iota
	IntResult
	DecimalResult
	RealResult
	RowResult
)

type ExprField struct {
	resTyp     ResultType
	decimal    int
	prec       int
	isUnsigned bool
}

func EvalBool(v sqltypes.Value) bool {
	v2, _ := sqltypes.NewValue(querypb.Type_INT64, []byte("0"))
	cmp := sqltypes.NullsafeCompare(v, v2)
	if cmp == 0 {
		return false
	}
	return true
}

func buildInt64Value(val int64) sqltypes.Value {
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, val)
	v, _ := sqltypes.NewValue(querypb.Type_INT64, bytesBuffer.Bytes())
	return v
}

func (expr *AndExpr) ResultType() ResultType {
	return IntResult
}

func (expr *AndExpr) FixField() *ExprField {
	return &ExprField{IntResult, 0, -1, false}
}

func (expr *AndExpr) Eval() ([]sqltypes.Value, bool, error) {
	v1, hasNull1, err := expr.Left.(*ParenExpr).Eval()
	if err != nil {
		return []sqltypes.Value{sqltypes.NULL}, true, err
	}
	if !hasNull1 && !EvalBool(v1[0]) {
		return []sqltypes.Value{buildInt64Value(0)}, false, err
	}

	v2, hasNull2, err := expr.Right.(*ParenExpr).Eval()
	if err != nil {
		return []sqltypes.Value{sqltypes.NULL}, true, err
	}
	if !hasNull1 && !EvalBool(v2[0]) {
		return []sqltypes.Value{buildInt64Value(0)}, false, err
	}

	if hasNull1 || hasNull2 {
		return []sqltypes.Value{sqltypes.NULL}, true, err
	}

	return []sqltypes.Value{buildInt64Value(1)}, false, nil
}

func (expr *OrExpr) ResultType() ResultType {
	return IntResult
}

func (expr *OrExpr) FixField() *ExprField {
	return &ExprField{IntResult, 0, -1, false}
}

func (expr *OrExpr) Eval() ([]sqltypes.Value, bool, error) {
	v1, hasNull1, err := expr.Left.(*ParenExpr).Eval()
	if err != nil {
		return []sqltypes.Value{sqltypes.NULL}, true, err
	}
	if !hasNull1 && EvalBool(v1[0]) {
		return []sqltypes.Value{buildInt64Value(1)}, false, err
	}

	v2, hasNull2, err := expr.Right.(*ParenExpr).Eval()
	if err != nil {
		return []sqltypes.Value{sqltypes.NULL}, true, err
	}
	if !hasNull1 && EvalBool(v2[0]) {
		return []sqltypes.Value{buildInt64Value(1)}, false, err
	}

	if hasNull1 || hasNull2 {
		return []sqltypes.Value{sqltypes.NULL}, true, err
	}

	return []sqltypes.Value{buildInt64Value(0)}, false, nil
}

func (expr *NotExpr) ResultType() ResultType {
	return IntResult
}

func (expr *NotExpr) FixField() *ExprField {
	return &ExprField{IntResult, 0, -1, false}
}

func (expr *NotExpr) Eval() ([]sqltypes.Value, bool, error) {
	v, hasNull, err := expr.Expr.(*ParenExpr).Eval()
	if err != nil || hasNull {
		return []sqltypes.Value{sqltypes.NULL}, true, err
	}
	if EvalBool(v[0]) {
		return []sqltypes.Value{buildInt64Value(0)}, false, err
	}

	return []sqltypes.Value{buildInt64Value(1)}, false, err
}

func (expr *ParenExpr) FixField() *ExprField {
	return expr.Expr.(*ParenExpr).FixField()
}

func (expr *ParenExpr) ResultType() ResultType {
	return expr.Expr.ResultType()
}

func (expr *ParenExpr) Eval() ([]sqltypes.Value, bool, error) {
	//return expr.Expr.Eval()
	return expr.Expr.(*ParenExpr).Eval()
}

func (expr *ComparisonExpr) FixField() *ExprField {
	return &ExprField{IntResult, 0, -1, false}
}

func (expr *ComparisonExpr) ResultType() ResultType {
	return IntResult
}

//注意NULL json怎么处理？
func (expr *ComparisonExpr) Eval() ([]sqltypes.Value, bool, error) {
	var v sqltypes.Value
	match := true
	hasNull := false
	// left, err := expr.Left.Eval()
	left, hasNull1, err := expr.Left.(*ParenExpr).Eval()
	if err != nil {
		return []sqltypes.Value{sqltypes.NULL}, true, err
	}
	// right, err := expr.Right.Eval()
	right, hasNull2, err := expr.Right.(*ParenExpr).Eval()
	if err != nil {
		return []sqltypes.Value{sqltypes.NULL}, true, err
	}
	switch expr.Operator {
	case EqualStr:
		if len(left) != len(right) {
			return []sqltypes.Value{sqltypes.NULL}, true, fmt.Errorf("unsupport: operand.should.contain.%d.column(s)", len(left))
		}

		if hasNull1 || hasNull2 {
			hasNull = true
			break
		}
		for i, v := range left {
			cmp := sqltypes.NullsafeCompare(v, right[i])
			if cmp != 0 {
				match = false
				break
			}
		}
	case LessThanStr:
		if len(left) != len(right) {
			return []sqltypes.Value{sqltypes.NULL}, true, fmt.Errorf("unsupport: operand.should.contain.%d.column(s)", len(left))
		}

		if hasNull1 || hasNull2 {
			hasNull = true
			break
		}
		for i, v := range left {
			cmp := sqltypes.NullsafeCompare(v, right[i])
			if cmp != -1 {
				match = false
				break
			}
		}
	case GreaterThanStr:
		if len(left) != len(right) {
			return []sqltypes.Value{sqltypes.NULL}, true, fmt.Errorf("unsupport: operand.should.contain.%d.column(s)", len(left))
		}

		if hasNull1 || hasNull2 {
			hasNull = true
			break
		}
		for i, v := range left {
			cmp := sqltypes.NullsafeCompare(v, right[i])
			if cmp != 1 {
				match = false
				break
			}
		}
	case LessEqualStr:
		if len(left) != len(right) {
			return []sqltypes.Value{sqltypes.NULL}, true, fmt.Errorf("unsupport: operand.should.contain.%d.column(s)", len(left))
		}

		if hasNull1 || hasNull2 {
			hasNull = true
			break
		}
		for i, v := range left {
			cmp := sqltypes.NullsafeCompare(v, right[i])
			if cmp == 1 {
				match = false
				break
			}
		}
	case GreaterEqualStr:
		if len(left) != len(right) {
			return []sqltypes.Value{sqltypes.NULL}, true, fmt.Errorf("unsupport: operand.should.contain.%d.column(s)", len(left))
		}

		if hasNull1 || hasNull2 {
			hasNull = true
			break
		}
		for i, v := range left {
			cmp := sqltypes.NullsafeCompare(v, right[i])
			if cmp == -1 {
				match = false
				break
			}
		}
	case NotEqualStr:
		if len(left) != len(right) {
			return []sqltypes.Value{sqltypes.NULL}, true, fmt.Errorf("unsupport: operand.should.contain.%d.column(s)", len(left))
		}

		if hasNull1 || hasNull2 {
			hasNull = true
			break
		}
		for i, v := range left {
			cmp := sqltypes.NullsafeCompare(v, right[i])
			if cmp == 0 {
				match = false
				break
			}
		}
	case NullSafeEqualStr:
		if len(left) != len(right) {
			return []sqltypes.Value{sqltypes.NULL}, true, fmt.Errorf("unsupport: operand.should.contain.%d.column(s)", len(left))
		}

		for i, v := range left {
			cmp := sqltypes.NullsafeCompare(v, right[i])
			if cmp != 0 {
				match = false
				break
			}
		}
	case InStr:
		if v, ok := expr.Right.(ValTuple)[0].(ValTuple); !ok && len(left) != 1 || ok && len(v) != len(left) {
			return []sqltypes.Value{sqltypes.NULL}, true, fmt.Errorf("unsupport: operand.should.contain.%d.column(s)", len(left))
		}

		if hasNull1 {
			hasNull = true
			break
		}

		i := 0
		for i < len(right) {
			find := true
			for j, v := range left {
				cmp := sqltypes.NullsafeCompare(v, right[i+j])
				if cmp != 0 {
					find = false
					break
				}
			}
			if !find {
				i = i + len(left)
			} else {
				match = true
				break
			}
		}

		if !match && hasNull2 {
			hasNull = true
		}
	case NotInStr:
		if v, ok := expr.Right.(ValTuple)[0].(ValTuple); !ok && len(left) != 1 || ok && len(v) != len(left) {
			return []sqltypes.Value{sqltypes.NULL}, true, fmt.Errorf("unsupport: operand.should.contain.%d.column(s)", len(left))
		}

		if hasNull1 {
			hasNull = true
			break
		}

		i := 0
		for i < len(right) {
			find := true
			for j, v := range left {
				cmp := sqltypes.NullsafeCompare(v, right[i+j])
				if cmp != 0 {
					find = false
					break
				}
			}
			if !find {
				i = i + len(left)
			} else {
				match = true
				break
			}
		}

		if !match {
			if hasNull2 {
				hasNull = true
			} else {
				match = true
			}
		} else {
			match = false
		}
	case LikeStr:
		if len(left) != 1 || len(right) != 1 {
			return []sqltypes.Value{sqltypes.NULL}, true, errors.New("unsupport: operand.should.contain.1.column(s)")
		}

		if hasNull1 || hasNull2 {
			hasNull = true
			break
		}

		escape := byte('\\')
		if expr.Escape != nil {
			escapes, hasNull3, err := expr.Escape.(*ParenExpr).Eval()
			if hasNull3 || err != nil || len(escapes) != 1 || escapes[0].Len() != 1 {
				return []sqltypes.Value{sqltypes.NULL}, true, errors.New("unsupport: incorrect.arguments.to.ESCAPE")
			}
			escape = escapes[0].Raw()[0]
		}

		cmpLike := sqltypes.NewCmpLike(right[0].Raw(), escape, false)
		match = cmpLike.Compare(left[0].Raw())
	case NotLikeStr:
		if len(left) != 1 || len(right) != 1 {
			return []sqltypes.Value{sqltypes.NULL}, true, errors.New("unsupport: operand.should.contain.1.column(s)")
		}

		if hasNull1 || hasNull2 {
			hasNull = true
			break
		}

		escape := byte('\\')
		if expr.Escape != nil {
			escapes, hasNull3, err := expr.Escape.(*ParenExpr).Eval()
			if hasNull3 || err != nil || len(escapes) != 1 || escapes[0].Len() != 1 {
				return []sqltypes.Value{sqltypes.NULL}, true, errors.New("unsupport: incorrect.arguments.to.ESCAPE")
			}
			escape = escapes[0].Raw()[0]
		}

		cmpLike := sqltypes.NewCmpLike(right[0].Raw(), escape, false)
		match = !cmpLike.Compare(left[0].Raw())
	case RegexpStr:
		if len(left) != 1 || len(right) != 1 {
			return []sqltypes.Value{sqltypes.NULL}, true, errors.New("unsupport: operand.should.contain.1.column(s)")
		}

		if hasNull1 || hasNull2 {
			hasNull = true
			break
		}

		reg, err := regexp.Compile(right[0].String())
		if err != nil {
			return []sqltypes.Value{sqltypes.NULL}, true, err
		}
		match = reg.Match(left[0].Raw())
	case NotRegexpStr:
		if len(left) != 1 || len(right) != 1 {
			return []sqltypes.Value{sqltypes.NULL}, true, errors.New("unsupport: operand.should.contain.1.column(s)")
		}

		if hasNull1 || hasNull2 {
			hasNull = true
			break
		}

		reg, err := regexp.Compile(right[0].String())
		if err != nil {
			return []sqltypes.Value{sqltypes.NULL}, true, err
		}
		match = !reg.Match(left[0].Raw())
	case JSONExtractOp:
		return []sqltypes.Value{sqltypes.NULL}, true, errors.New("unsupport: json_extract")
	case JSONUnquoteExtractOp:
		return []sqltypes.Value{sqltypes.NULL}, true, errors.New("unsupport: json_extract")
	}
	if hasNull {
		v = sqltypes.NULL
	} else {
		if match {
			v = buildInt64Value(1)
		} else {
			v = buildInt64Value(0)
		}
	}
	return []sqltypes.Value{v}, hasNull, nil
}

func (expr *RangeCond) FixField() *ExprField {
	return &ExprField{IntResult, 0, -1, false}
}

func (expr *RangeCond) ResultType() ResultType {
	return IntResult
}

func (expr *RangeCond) Eval() ([]sqltypes.Value, bool, error) {
	val, hasNull, err := expr.Left.(*ParenExpr).Eval()
	if err != nil || hasNull {
		return []sqltypes.Value{sqltypes.NULL}, true, err
	}
	// left, err := expr.Left.Eval()
	left, hasNull, err := expr.From.(*ParenExpr).Eval()
	if err != nil || hasNull {
		return []sqltypes.Value{sqltypes.NULL}, true, err
	}
	// right, err := expr.Right.Eval()
	right, hasNull, err := expr.To.(*ParenExpr).Eval()
	if err != nil || hasNull {
		return []sqltypes.Value{sqltypes.NULL}, true, err
	}

	if len(val) != 1 || len(left) != 1 || len(right) != 1 {
		return []sqltypes.Value{sqltypes.NULL}, true, errors.New("unsupport: operand.should.contain.1.column(s)")
	}

	cmp1 := sqltypes.NullsafeCompare(val[0], left[0])
	cmp2 := sqltypes.NullsafeCompare(val[0], right[0])

	if cmp1 >= 0 && cmp2 <= 0 {
		return []sqltypes.Value{buildInt64Value(1)}, false, nil
	}
	return []sqltypes.Value{buildInt64Value(0)}, false, nil
}

func (expr *IsExpr) FixField() *ExprField {
	return &ExprField{IntResult, 0, -1, false}
}

func (expr *IsExpr) ResultType() ResultType {
	return IntResult
}

func (expr *IsExpr) Eval() ([]sqltypes.Value, bool, error) {
	val, hasNull, err := expr.Expr.(*ParenExpr).Eval()
	if err != nil {
		return []sqltypes.Value{sqltypes.NULL}, true, err
	}
	if len(val) != 1 {
		return []sqltypes.Value{sqltypes.NULL}, true, errors.New("unsupport: operand.should.contain.1.column(s)")
	}

	match := false
	switch expr.Operator {
	case IsNullStr:
		if hasNull {
			match = true
		}
	case IsNotNullStr:
		if !hasNull {
			match = true
		}
	case IsTrueStr:
		if hasNull {
			match = false
		} else {
			match = EvalBool(val[0])
		}
	case IsNotTrueStr:
		if hasNull {
			match = true
		} else {
			match = !EvalBool(val[0])
		}
	case IsFalseStr:
		if hasNull {
			match = false
		} else {
			match = !EvalBool(val[0])
		}
	case IsNotFalseStr:
		if hasNull {
			match = true
		} else {
			match = EvalBool(val[0])
		}
	}

	if match {
		return []sqltypes.Value{buildInt64Value(1)}, false, nil
	}
	return []sqltypes.Value{buildInt64Value(0)}, false, nil
}

func (expr *ExistsExpr) FixField() *ExprField {
	return &ExprField{IntResult, 0, -1, false}
}

func (expr *ExistsExpr) ResultType() ResultType {
	return IntResult
}

func (expr *ExistsExpr) Eval() ([]sqltypes.Value, bool, error) {
	return []sqltypes.Value{sqltypes.NULL}, true, errors.New("unsupport: exists.subquery")
}

func (expr *SQLVal) FixField() *ExprField {
	var resTyp ResultType
	decimal := 0
	prec := -1
	if _, err := strconv.ParseFloat(string(expr.Val), 64); err == nil {
		sub := strings.Split(string(expr.Val), ".")
		if len(sub) == 2 {
			decimal = len(sub[1])
		}
	}
	switch expr.Type {
	case IntVal, HexNum:
		resTyp = IntResult
	case StrVal, HexVal, ValArg:
		resTyp = StringResult
		if decimal != 0 {
			prec = decimal
		}
		decimal = NotFixedDec
	case FloatVal:
		resTyp = DecimalResult
		prec = decimal
	}

	return &ExprField{resTyp, decimal, prec, false}
}

func (expr *SQLVal) ResultType() ResultType {
	switch expr.Type {
	case IntVal, HexNum:
		return IntResult
	case StrVal, HexVal, ValArg:
		return StringResult
	case FloatVal:
		return DecimalResult
	}
	panic("unsupported sql value")
}

func (expr *SQLVal) Eval() ([]sqltypes.Value, bool, error) {
	var v sqltypes.Value
	var err error
	val := common.BytesToString(expr.Val)
	switch expr.Type {
	case HexNum:
		_, err2 := strconv.ParseUint(val, 16, 64)
		if err2 == nil {
			v = sqltypes.MakeTrusted(sqltypes.Uint64, expr.Val)
		} else {
			err = err2
		}
	case IntVal:
		_, err1 := strconv.ParseInt(val, 0, 64)
		if err1 == nil {
			v = sqltypes.MakeTrusted(sqltypes.Int64, expr.Val)
			break
		}

		_, err2 := strconv.ParseUint(val, 0, 64)
		if err2 == nil {
			v = sqltypes.MakeTrusted(sqltypes.Uint64, expr.Val)
		} else {
			err = err2
		}
	case HexVal:
		codes, err1 := expr.HexDecode()
		if err1 != nil {
			err = err1
			break
		}
		v = sqltypes.MakeTrusted(sqltypes.VarBinary, codes)
	case FloatVal:
		v, err = sqltypes.NewValue(sqltypes.Decimal, expr.Val)
	case StrVal, ValArg:
		v, err = sqltypes.NewValue(sqltypes.VarChar, expr.Val)
	}
	return []sqltypes.Value{v}, v.IsNull(), err
}

func (expr *NullVal) FixField() *ExprField {
	return &ExprField{StringResult, NotFixedDec, -1, false}
}

func (expr *NullVal) ResultType() ResultType {
	return StringResult
}

func (expr *NullVal) Eval() ([]sqltypes.Value, bool, error) {
	return []sqltypes.Value{sqltypes.NULL}, true, nil
}

func (expr BoolVal) FixField() *ExprField {
	return &ExprField{IntResult, 0, -1, false}
}

func (expr BoolVal) ResultType() ResultType {
	return IntResult
}

func (expr BoolVal) Eval() ([]sqltypes.Value, bool, error) {
	if expr {
		return []sqltypes.Value{buildInt64Value(1)}, false, nil
	}
	return []sqltypes.Value{buildInt64Value(0)}, false, nil
}

func (expr *ColName) FixField() *ExprField {
	var resTyp ResultType
	prec := -1
	if expr.Metadata == nil {
		return &ExprField{StringResult, NotFixedDec, prec, false}
	}

	col := expr.Metadata.(*Column)
	typ := col.Val.Type()
	if sqltypes.IsIntegral(typ) {
		resTyp = IntResult
	} else if sqltypes.IsFloat(typ) {
		resTyp = RealResult
	} else if typ == sqltypes.Decimal {
		resTyp = DecimalResult
		prec = col.Decimal
	} else if sqltypes.IsTemporal(typ) {
		if col.Decimal == 0 {
			resTyp = IntResult
		} else {
			resTyp = DecimalResult
			prec = col.Decimal
		}
	} else {
		resTyp = StringResult
	}

	isUnsigned := false
	if sqltypes.IsUnsigned(typ) {
		isUnsigned = true
	}

	return &ExprField{resTyp, col.Decimal, prec, isUnsigned}
}

func (expr *ColName) ResultType() ResultType {
	if expr.Metadata == nil {
		return StringResult
	}
	typ := expr.Metadata.(Column).Val.Type()
	if sqltypes.IsIntegral(typ) {
		return IntResult
	}
	if sqltypes.IsFloat(typ) {
		return RealResult
	}
	if typ == sqltypes.Decimal {
		return DecimalResult
	}
	if sqltypes.IsTemporal(typ) {
		if expr.Metadata.(*Column).Decimal == 0 {
			return IntResult
		}
		return DecimalResult
	}
	return StringResult
}

func (expr *ColName) Eval() ([]sqltypes.Value, bool, error) {
	return []sqltypes.Value{expr.Metadata.(*Column).Val}, expr.Metadata.(*Column).Val.IsNull(), nil
}

func (expr ValTuple) FixField() *ExprField {
	return &ExprField{RowResult, 0, -1, false}
}

func (expr ValTuple) ResultType() ResultType {
	return RowResult
}

func (expr ValTuple) Eval() ([]sqltypes.Value, bool, error) {
	var vals []sqltypes.Value
	hasNull := false
	for _, exp := range expr {
		v, isNull, err := exp.(*ParenExpr).Eval()
		if err != nil {
			return []sqltypes.Value{sqltypes.NULL}, true, err
		}
		if isNull {
			hasNull = true
		}
		vals = append(vals, v...)
	}
	return vals, hasNull, nil
}

func (expr *Subquery) FixField() *ExprField {
	panic("unreachable")
}

func (expr *Subquery) ResultType() ResultType {
	panic("unreachable")
}

func (expr *Subquery) Eval() ([]sqltypes.Value, bool, error) {
	return []sqltypes.Value{sqltypes.NULL}, true, errors.New("unsupport: Subquery")
}

func (expr ListArg) FixField() *ExprField {
	return &ExprField{StringResult, NotFixedDec, -1, false}
}

func (expr ListArg) ResultType() ResultType {
	return StringResult
}

func (expr ListArg) Eval() ([]sqltypes.Value, bool, error) {
	return []sqltypes.Value{sqltypes.MakeTrusted(sqltypes.VarChar, expr)}, false, nil
}

func (expr *BinaryExpr) FixField() *ExprField {
	var typ ResultType
	left := expr.Left.(*ParenExpr).FixField()
	right := expr.Right.(*ParenExpr).FixField()
	if left.resTyp == RowResult || right.resTyp == RowResult {
		panic("unsupport.result.type")
	}

	decimal := 0
	prec := -1
	isUnsigned := false
	switch expr.Operator {
	case ShiftLeftStr, ShiftRightStr, BitAndStr, BitOrStr, BitXorStr:
		typ = IntResult
	case IntDivStr:
		typ = IntResult
		isUnsigned = left.isUnsigned || right.isUnsigned
	case PlusStr, MinusStr, MultStr, DivStr, ModStr:
		if left.resTyp == StringResult || left.resTyp == RealResult || right.resTyp == StringResult || right.resTyp == RealResult {
			typ = RealResult
		} else if left.resTyp == DecimalResult || right.resTyp == DecimalResult {
			typ = DecimalResult
		} else {
			switch expr.Operator {
			case DivStr:
				typ = DecimalResult
			case ModStr:
				typ = left.resTyp
				isUnsigned = left.isUnsigned
			default:
				typ = IntResult
				isUnsigned = left.isUnsigned || right.isUnsigned
			}
		}
	}

	switch expr.Operator {
	case PlusStr, MinusStr, ModStr:
		decimal = common.Max(left.decimal, right.decimal)
	case MultStr:
		decimal = left.decimal + right.decimal
	case DivStr:
		decimal = left.decimal + 4
	}

	if typ == RealResult {
		decimal = NotFixedDec
	}
	if typ == DecimalResult {
		decimal = common.Min(DecimalMaxScale, decimal)
		prec = decimal
	}

	return &ExprField{typ, decimal, prec, isUnsigned}
}

func (expr *BinaryExpr) ResultType() ResultType {
	var typ ResultType
	lr := expr.Left.ResultType()
	rr := expr.Right.ResultType()
	if lr == RowResult || rr == RowResult {
		panic("unsupport.result.type")
	}

	switch expr.Operator {
	case ShiftLeftStr, ShiftRightStr, BitAndStr, IntDivStr, BitOrStr, BitXorStr:
		typ = IntResult
	case PlusStr, MinusStr, MultStr, DivStr, ModStr:
		if lr == StringResult || lr == RealResult || rr == StringResult || rr == RealResult {
			typ = RealResult
		} else if lr == DecimalResult || rr == DecimalResult {
			typ = DecimalResult
		} else {
			if expr.Operator == DivStr {
				typ = DecimalResult
			} else {
				typ = IntResult
			}
		}
	}
	return typ
}

func (expr *BinaryExpr) Eval() ([]sqltypes.Value, bool, error) {
	var v sqltypes.Value
	var err error
	// left, err := expr.Left.Eval()
	left, hasNull1, err := expr.Left.(*ParenExpr).Eval()
	if err != nil {
		return []sqltypes.Value{sqltypes.NULL}, true, err
	}
	// right, err := expr.Right.Eval()
	right, hasNull2, err := expr.Right.(*ParenExpr).Eval()
	if err != nil {
		return []sqltypes.Value{sqltypes.NULL}, true, err
	}

	if len(left) != 1 || len(right) != 1 {
		return []sqltypes.Value{sqltypes.NULL}, true, errors.New("unsupport: operand.should.contain.1.column(s)")
	}
	if hasNull1 || hasNull2 {
		return []sqltypes.Value{sqltypes.NULL}, true, nil
	}

	exprField := expr.FixField()
	var resTyp querypb.Type
	switch exprField.resTyp {
	case IntResult:
		if exprField.isUnsigned {
			resTyp = sqltypes.Uint64
		} else {
			resTyp = sqltypes.Int64
		}
	case RealResult:
		resTyp = sqltypes.Float64
	case DecimalResult:
		resTyp = sqltypes.Decimal
	}

	switch expr.Operator {
	case BitAndStr:
		v, err = sqltypes.NullsafeBitAnd(left[0], right[0])
	case BitOrStr:
		v, err = sqltypes.NullsafeBitOr(left[0], right[0])
	case BitXorStr:
		v, err = sqltypes.NullsafeBitXor(left[0], right[0])
	case PlusStr:
		v, err = sqltypes.NullsafeAdd(left[0], right[0], resTyp, exprField.prec)
	case MinusStr:
		v, err = sqltypes.NullsafeMinus(left[0], right[0], resTyp, exprField.prec)
	case MultStr:
		v, err = sqltypes.NullsafeMulti(left[0], right[0], resTyp, exprField.prec)
	case DivStr:
		v, err = sqltypes.NullsafeDiv(left[0], right[0], resTyp, exprField.prec)
	case IntDivStr:
		v, err = sqltypes.NullsafeIntDiv(left[0], right[0], resTyp, exprField.prec, exprField.isUnsigned)
	case ModStr:
		v, err = sqltypes.NullsafeMod(left[0], right[0], resTyp, exprField.prec)
	case ShiftLeftStr:
		v, err = sqltypes.NullsafeShiftLeft(left[0], right[0])
	case ShiftRightStr:
		v, err = sqltypes.NullsafeShiftRight(left[0], right[0])
	}
	return []sqltypes.Value{v}, v.IsNull(), err
}

func (expr *UnaryExpr) FixField() *ExprField {
	var typ ResultType
	decimal := 0
	prec := -1
	isUnsigned := false
	exprFld := expr.Expr.(*ParenExpr).FixField()
	//typ
	switch expr.Operator {
	case UPlusStr:
		return exprFld
	case UMinusStr:
		typ = exprFld.resTyp
		decimal = exprFld.decimal
		if exprFld.resTyp == StringResult {
			typ = RealResult
			decimal = NotFixedDec
		}
	case TildaStr:
		isUnsigned = true
		typ = IntResult
	case BangStr:
		typ = IntResult
	case BinaryStr:
		typ = StringResult
		decimal = NotFixedDec
	}

	if typ == DecimalResult {
		prec = decimal
	}
	return &ExprField{typ, decimal, prec, isUnsigned}
}

func (node *UnaryExpr) ResultType() ResultType {
	return node.Expr.ResultType()
}

func (expr *UnaryExpr) Eval() ([]sqltypes.Value, bool, error) {
	if expr.Operator == UPlusStr {
		return expr.Expr.(*ParenExpr).Eval()
	}
	var v sqltypes.Value
	var err error
	exprRes, hasNull, err := expr.Expr.(*ParenExpr).Eval()
	if err != nil || hasNull {
		return []sqltypes.Value{sqltypes.NULL}, true, err
	}

	if len(exprRes) != 1 {
		return []sqltypes.Value{sqltypes.NULL}, true, errors.New("unsupport: operand.should.contain.1.column(s)")
	}

	exprField := expr.FixField()
	var resTyp querypb.Type
	switch exprField.resTyp {
	case IntResult:
		if exprField.isUnsigned {
			resTyp = sqltypes.Uint64
		} else {
			resTyp = sqltypes.Int64
		}
	case RealResult:
		resTyp = sqltypes.Float64
	case DecimalResult:
		resTyp = sqltypes.Decimal
	case StringResult:
		resTyp = sqltypes.VarBinary
	}

	switch expr.Operator {
	case UMinusStr:

	case TildaStr:

	case BangStr:

	case BinaryStr:
	}
	return []sqltypes.Value{v}, v.IsNull(), err
}
