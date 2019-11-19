/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"router"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

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
	var comments string
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

	err := route.AddForTest("sbtest", router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableSConfig(), router.MockTableGConfig(),
		router.MockTableG1Config(), router.MockTableRConfig(), router.MockTableListConfig(), router.MockTableCConfig(), router.MockTableList1Config())
	assert.Nil(t, err)

	for tcase := range iterateExecFile(filename) {
		t.Run(tcase.comments, func(t *testing.T) {
			node, err := sqlparser.Parse(tcase.input)
			assert.Nil(t, err)

			plan, err := BuildNode(log, route, database, node.(sqlparser.SelectStatement))
			var out string
			if err != nil {
				out = err.Error()
			} else {
				if tcase.output == "pass" {
					return
				}
				out = strings.Replace(JSON(plan), "\t", "    ", -1)
				plan.Children()
			}
			if out != tcase.output {
				t.Errorf("File: %s, Line:%v\n got:\n%s, \nwant:\n%s", filename, tcase.lineno, out, tcase.output)
			}
		})
	}
}

func TestBuilder(t *testing.T) {
	testFile(t, "database_null_cases.txt", "")
	testFile(t, "select_cases.txt", "sbtest")
	testFile(t, "union_cases.txt", "sbtest")
	testFile(t, "unsupported_cases.txt", "sbtest")
}

func TestUnsportStatement(t *testing.T) {
	query := "select a from A where id = 10 union (select a from B where id=3)"

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig())
	assert.Nil(t, err)

	databaseNull := ""
	node, err := sqlparser.Parse(query)
	assert.Nil(t, err)
	_, err = BuildNode(log, route, databaseNull, node.(*sqlparser.Union).Right)
	want := "unsupported: unknown.select.statement"
	got := err.Error()
	assert.Equal(t, want, got)
}

func TestGenerateFieldQuery(t *testing.T) {
	query := "select A.id+B.id from A join B on A.name=B.name"
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig())
	assert.Nil(t, err)

	node, err := sqlparser.Parse(query)
	assert.Nil(t, err)
	plan, err := BuildNode(log, route, database, node.(sqlparser.SelectStatement))
	assert.Nil(t, err)

	got := plan.(*JoinNode).Right.(*MergeNode).GenerateFieldQuery().Query
	want := "select :A_id + B.id as `A.id + B.id` from sbtest.B1 as B where 1 != 1"
	assert.Equal(t, want, got)
}
