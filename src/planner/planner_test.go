/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"router"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestPlanner(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	database := "xx"
	query := "create table A(a int)"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableAConfig())
	assert.Nil(t, err)

	node, err := sqlparser.Parse(query)
	assert.Nil(t, err)
	DDL := NewDDLPlan(log, database, query, node.(*sqlparser.DDL), route)

	{
		planTree := NewPlanTree()
		for i := 0; i < 64; i++ {
			err := planTree.Add(DDL)
			assert.Nil(t, err)
		}
		err := planTree.Build()
		assert.Nil(t, err)
		planSize := planTree.Size()
		log.Info("planSize: %v", planSize)
		len := len(planTree.Plans())
		assert.Equal(t, 64, len)
	}
}

func TestPlannerError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	database := "xx"
	query := "create table A(a int)"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableAConfig())
	assert.Nil(t, err)

	node, err := sqlparser.Parse(query)
	assert.Nil(t, err)
	database1 := ""
	DDL := NewDDLPlan(log, database1, query, node.(*sqlparser.DDL), route)

	{
		planTree := NewPlanTree()
		for i := 0; i < 64; i++ {
			err := planTree.Add(DDL)
			assert.Nil(t, err)
		}
		err := planTree.Build()
		assert.NotNil(t, err)
	}
}

type testCase struct {
	file     string
	lineno   int
	typ      string
	input    string
	output   string
	comments string
}

func iterateExecFile(name string) (testCaseIterator chan testCase) {
	name = locateFile(name)
	fd, err := os.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		panic(fmt.Sprintf("Could not open file %s", name))
	}
	testCaseIterator = make(chan testCase)
	var comments, typ string
	go func() {
		defer close(testCaseIterator)

		r := bufio.NewReader(fd)
		lineno := 0
		for {
			binput, err := r.ReadBytes('\n')
			if err != nil {
				if err != io.EOF {
					fmt.Printf("Line: %d\n", lineno)
					panic(fmt.Errorf("error reading file %s: %s", name, err.Error()))
				}
				break
			}
			lineno++
			input := string(binput)
			if input == "" || input == "\n" {
				continue
			}
			if input[0] == '#' {
				comments = comments + input
				continue
			}
			if strings.HasPrefix(input, "\"Type\":") {
				typ = input[9 : len(input)-2]
				continue
			}
			err = json.Unmarshal(binput, &input)
			if err != nil {
				fmt.Printf("Line: %d, input: %s\n", lineno, binput)
				panic(err)
			}
			input = strings.Trim(input, "\"")
			var output []byte
			for {
				l, err := r.ReadBytes('\n')
				lineno++
				if err != nil {
					fmt.Printf("Line: %d\n", lineno)
					panic(fmt.Errorf("error reading file %s: %s", name, err.Error()))
				}
				output = append(output, l...)
				if l[0] == '}' {
					output = output[:len(output)-1]
					break
				}
				if l[0] == '"' {
					output = output[1 : len(output)-2]
					break
				}
			}
			testCaseIterator <- testCase{
				file:     name,
				lineno:   lineno,
				typ:      typ,
				input:    input,
				output:   string(output),
				comments: comments,
			}
			comments = ""
		}
	}()
	return testCaseIterator
}

func locateFile(name string) string {
	return "testdata/" + name
}

func testFile(t *testing.T, filename string, database string) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest("sbtest", router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableSConfig(), router.MockTableGConfig(), router.MockTableRConfig())
	assert.Nil(t, err)

	for tcase := range iterateExecFile(filename) {
		t.Run(tcase.comments, func(t *testing.T) {
			node, err := sqlparser.Parse(tcase.input)
			assert.Nil(t, err)

			var plan Plan
			switch nod := node.(type) {
			case *sqlparser.DDL:
				for _, tableIdent := range nod.Tables {
					if !tableIdent.Qualifier.IsEmpty() {
						database = tableIdent.Qualifier.String()
					}
					nod.Table = tableIdent
				}
				plan = NewDDLPlan(log, database, tcase.input, nod, route)
			case *sqlparser.Insert:
				plan = NewInsertPlan(log, database, tcase.input, nod, route)
			case *sqlparser.Delete:
				plan = NewDeletePlan(log, database, tcase.input, nod, route)
			case *sqlparser.Update:
				plan = NewUpdatePlan(log, database, tcase.input, nod, route)
			case *sqlparser.Select:
				plan = NewSelectPlan(log, database, tcase.input, nod, route)
			case *sqlparser.Union:
				plan = NewUnionPlan(log, database, tcase.input, nod, route)
			case *sqlparser.Checksum:
				plan = NewOthersPlan(log, database, tcase.input, nod, route)
			}

			var out string
			err = plan.Build()
			if err != nil {
				out = err.Error()
			} else {
				out = strings.Replace(plan.JSON(), "\t", "    ", -1)
				assert.Equal(t, tcase.typ, string(plan.Type()))
				plan.Size()
			}

			if out != tcase.output {
				t.Errorf("File: %s, Line:%v\n got:\n%s, \nwant:\n%s", filename, tcase.lineno, out, tcase.output)
			}
		})
	}
}

func TestPlan(t *testing.T) {
	testFile(t, "database_null_cases.txt", "")
	testFile(t, "ddl_cases.txt", "sbtest")
	testFile(t, "dml_cases.txt", "sbtest")
	testFile(t, "other_cases.txt", "sbtest")
	testFile(t, "unsupported_cases.txt", "sbtest")
}

func TestInsertPlanBench(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	query := "insert into sbtest.A(id, b, c) values(1,2,3),(23,4,5), (117,3,4),(1,2,3),(23,4,5), (117,3,4)"
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig())
	assert.Nil(t, err)

	{
		N := 100000
		now := time.Now()
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		for i := 0; i < N; i++ {
			plan := NewInsertPlan(log, database, query, node.(*sqlparser.Insert), route)
			err := plan.Build()
			assert.Nil(t, err)
		}

		took := time.Since(now)
		fmt.Printf(" LOOP\t%v COST %v, avg:%v/s\n", N, took, (int64(N)/(took.Nanoseconds()/1e6))*1000)
	}
}

func TestReplacePlanBench(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	query := "replace into sbtest.A(id, b, c) values(1,2,3),(23,4,5), (117,3,4),(1,2,3),(23,4,5), (117,3,4)"
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig())
	assert.Nil(t, err)

	{
		N := 100000
		now := time.Now()
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		for i := 0; i < N; i++ {
			plan := NewInsertPlan(log, database, query, node.(*sqlparser.Insert), route)
			err := plan.Build()
			assert.Nil(t, err)
		}

		took := time.Since(now)
		fmt.Printf(" LOOP\t%v COST %v, avg:%v/s\n", N, took, (int64(N)/(took.Nanoseconds()/1e6))*1000)
	}
}
