/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package logic

import (
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

type route struct {
	log          *xlog.Log
	nonGlobalCnt int
	indexes      []int
	Select       sqlparser.SelectStatement
	// referred tables' tableInfo map.
	referTables map[string]*tables
}

func newRoute() *route {
	return &route{}
}

func (*route) test() {

}

type joiner struct {
	
}

func newJoiner() *joiner {
	return &joiner{}
}

func (*joiner) test() {

}

type node interface {
	test()
}
