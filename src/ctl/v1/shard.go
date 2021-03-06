/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package v1

import (
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"proxy"

	"github.com/ant0ine/go-json-rest/rest"
	"github.com/radondb/shift/shift"
	shiftlog "github.com/radondb/shift/xlog"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	subtable = regexp.MustCompile("_[0-9]{4}$")
)

// SubTableToTable used to determine from is subtable or not; if it is, get the table from the subtable.
func SubTableToTable(from string) (isSub bool, to string) {
	isSub = false
	to = ""

	Suffix := subtable.FindAllStringSubmatch(from, -1)
	lenSuffix := len(Suffix)
	if lenSuffix == 0 {
		return
	}

	isSub = true
	to = strings.TrimSuffix(from, Suffix[0][lenSuffix-1])
	return
}

// ShardzHandler impl.
func ShardzHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		shardzHandler(log, proxy, w, r)
	}
	return f
}

func shardzHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	router := proxy.Router()
	rulez := router.Rules()
	w.WriteJson(rulez)
}

// ShardBalanceAdviceHandler impl.
func ShardBalanceAdviceHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		shardBalanceAdviceHandler(log, proxy, w, r)
	}
	return f
}

// shardBalanceAdviceHandler used to get the advice who will be transfered.
// The Find algorithm as follows:
//
// 1. find the max datasize backend and min datasize backend.
//    1.1 max-datasize - min.datasize > 1GB
//    1.2 transfer path is: max --> min
//
// 2. find the best table(advice-table) to tansfer:
//    2.1 max.datasize - advice-table-size > min.datasize + advice-table-size
//
// Returns:
// 1. Status:200, Body:null
// 2. Status:503
// 3. Status:200, Body:JSON
func shardBalanceAdviceHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	scatter := proxy.Scatter()
	spanner := proxy.Spanner()
	backends := scatter.Backends()

	type backendSize struct {
		name    string
		address string
		size    float64
		user    string
		passwd  string
	}

	// 1. Find the max and min backend.
	var max, min backendSize
	for _, backend := range backends {
		query := "select round((sum(data_length) + sum(index_length)) / 1024/ 1024, 0)  as SizeInMB from information_schema.tables"
		qr, err := spanner.ExecuteOnThisBackend(backend, query)
		if err != nil {
			log.Error("api.v1.balance.advice.backend[%s].error:%+v", backend, err)
			rest.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if len(qr.Rows) > 0 {
			valStr := string(qr.Rows[0][0].Raw())
			datasize, err := strconv.ParseFloat(valStr, 64)
			if err != nil {
				log.Error("api.v1.balance.advice.parse.value[%s].error:%+v", valStr, err)
				rest.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			if datasize > max.size {
				max.name = backend
				max.size = datasize
			}

			if min.size == 0 {
				min.name = backend
				min.size = datasize
			}
			if datasize < min.size {
				min.name = backend
				min.size = datasize
			}
		}
	}
	log.Warning("api.v1.balance.advice.max:[%+v], min:[%+v]", max, min)

	// The differ must big than 256MB.
	delta := float64(256)
	differ := (max.size - min.size)
	if differ < delta {
		log.Warning("api.v1.balance.advice.return.nil.since.differ[%+vMB].less.than.%vMB", differ, delta)
		w.WriteJson(nil)
		return
	}

	backendConfs := scatter.BackendConfigsClone()
	for _, bconf := range backendConfs {
		if bconf.Name == max.name {
			max.address = bconf.Address
			max.user = bconf.User
			max.passwd = bconf.Password
		} else if bconf.Name == min.name {
			min.address = bconf.Address
			min.user = bconf.User
			min.passwd = bconf.Password
		}
	}

	// 2. Find the best table.
	query := "SELECT table_schema, table_name, ROUND((SUM(data_length+index_length)) / 1024/ 1024, 0) AS sizeMB FROM information_schema.TABLES GROUP BY table_name HAVING SUM(data_length + index_length)>10485760 ORDER BY (data_length + index_length) DESC"
	qr, err := spanner.ExecuteOnThisBackend(max.name, query)
	if err != nil {
		log.Error("api.v1.balance.advice.get.max[%+v].tables.error:%+v", max, err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var tableSize float64
	var database, table string
	route := proxy.Router()
	for _, row := range qr.Rows {
		db := string(row[0].Raw())
		tbl := string(row[1].Raw())
		valStr := string(row[2].Raw())
		tblSize, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			log.Error("api.v1.balance.advice.get.tables.parse.value[%s].error:%+v", valStr, err)
			rest.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Make sure the table is small enough.
		if (min.size + tblSize) < (max.size - tblSize) {
			isSub, t := SubTableToTable(tbl)
			if isSub {
				partitionType, err := route.PartitionType(db, t)
				// The advice table just hash, Filter the global/single/list table.
				if err == nil && route.IsPartitionHash(partitionType) {
					//Find the advice table.
					database = db
					table = tbl
					tableSize = tblSize
					break
				}
			}

			log.Warning("api.v1.balance.advice.skip.table[%v]", tbl)
		}
	}

	// No best.
	if database == "" || table == "" {
		log.Warning("api.v1.balance.advice.return.nil.since.cant.find.the.best.table")
		w.WriteJson(nil)
		return
	}

	type balanceAdvice struct {
		From         string  `json:"from-address"`
		FromDataSize float64 `json:"from-datasize"`
		FromUser     string  `json:"from-user"`
		FromPasswd   string  `json:"from-password"`
		To           string  `json:"to-address"`
		ToDataSize   float64 `json:"to-datasize"`
		ToUser       string  `json:"to-user"`
		ToPasswd     string  `json:"to-password"`
		Database     string  `json:"database"`
		Table        string  `json:"table"`
		TableSize    float64 `json:"tablesize"`
	}

	advice := balanceAdvice{
		From:         max.address,
		FromDataSize: max.size,
		FromUser:     max.user,
		FromPasswd:   max.passwd,
		To:           min.address,
		ToDataSize:   min.size,
		ToUser:       min.user,
		ToPasswd:     min.passwd,
		Database:     database,
		Table:        table,
		TableSize:    tableSize,
	}
	log.Warning("api.v1.balance.advice.return:%+v", advice)
	w.WriteJson(advice)
}

type ruleParams struct {
	Database    string `json:"database"`
	Table       string `json:"table"`
	FromAddress string `json:"from-address"`
	ToAddress   string `json:"to-address"`
}

// ShardRuleShiftHandler used to shift a partition rule to another backend.
func ShardRuleShiftHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		shardRuleShiftHandler(log, proxy, w, r)
	}
	return f
}

var sysDBs = []string{"information_schema", "mysql", "performance_schema", "sys"}

func shardRuleShiftHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	router := proxy.Router()
	scatter := proxy.Scatter()
	p := ruleParams{}
	err := r.DecodeJsonPayload(&p)
	if err != nil {
		log.Error("api.v1.radon.shard.rule.parse.json.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Warning("api.v1.radon.shard.rule[from:%v].request:%+v", r.RemoteAddr, p)

	if p.Database == "" || p.Table == "" {
		rest.Error(w, "api.v1.shard.rule.request.database.or.table.is.null", http.StatusInternalServerError)
		return
	}

	for _, sysDB := range sysDBs {
		if sysDB == strings.ToLower(p.Database) {
			log.Error("api.v1.shard.rule.database[%s].is.system", p.Database)
			rest.Error(w, "api.v1.shard.rule.database.can't.be.system.database", http.StatusInternalServerError)
			return
		}
	}

	var fromBackend, toBackend string
	backends := scatter.BackendConfigsClone()
	for _, backend := range backends {
		if backend.Address == p.FromAddress {
			fromBackend = backend.Name
		} else if backend.Address == p.ToAddress {
			toBackend = backend.Name
		}
	}

	if fromBackend == "" || toBackend == "" {
		log.Error("api.v1.shard.rule.fromBackend[%s].or.toBackend[%s].is.NULL", fromBackend, toBackend)
		rest.Error(w, "api.v1.shard.rule.backend.NULL", http.StatusInternalServerError)
		return
	}

	if err := router.PartitionRuleShift(fromBackend, toBackend, p.Database, p.Table); err != nil {
		log.Error("api.v1.shard.rule.PartitionRuleShift.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// ShardReLoadHandler impl.
func ShardReLoadHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		shardReLoadHandler(log, proxy, w, r)
	}
	return f
}

func shardReLoadHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	router := proxy.Router()
	log.Warning("api.shard.reload.prepare.from[%v]...", r.RemoteAddr)
	if err := router.ReLoad(); err != nil {
		log.Error("api.v1.shard.reload.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Warning("api.shard.reload.done...")
}

// GlobalsHandler used to get the global tables.
func GlobalsHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		globalsHandler(log, proxy, w, r)
	}
	return f
}

func globalsHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	router := proxy.Router()

	type databases struct {
		Database string   `json:"database"`
		Tables   []string `json:"tables"`
	}

	type schemas struct {
		Schemas []databases `json:"schemas"`
	}

	var globals schemas
	for _, schema := range router.Schemas {
		var tables []string
		for _, tb := range schema.Tables {
			if tb.TableConfig.ShardType == "GLOBAL" {
				tables = append(tables, tb.Name)
			}
		}
		if len(tables) > 0 {
			db := databases{
				Database: schema.DB,
				Tables:   tables,
			}
			globals.Schemas = append(globals.Schemas, db)
		}
	}

	if len(globals.Schemas) == 0 {
		log.Warning("api.v1.globals.return.nil.since.cant.find.the.global.tables")
		w.WriteJson(nil)
		return
	}
	w.WriteJson(globals)
}

type migrateParams struct {
	ToFlavor string `json:"to-flavor"`

	From         string `json:"from"`
	FromUser     string `json:"from-user"`
	FromPassword string `json:"from-password"`
	FromDatabase string `json:"from-database"`
	FromTable    string `json:"from-table"`

	To         string `json:"to"`
	ToUser     string `json:"to-user"`
	ToPassword string `json:"to-password"`
	ToDatabase string `json:"to-database"`
	ToTable    string `json:"to-table"`

	RadonURL               string `json:"radonurl"`
	Cleanup                bool   `json:"cleanup"`
	MySQLDump              string `json:"mysqldump"`
	Threads                int    `json:"threads"`
	Behinds                int    `json:"behinds"`
	Checksum               bool   `json:"checksum"`
	WaitTimeBeforeChecksum int    `json:"wait-time-before-checksum"`
}

// ShardMigrateHandler used to migrate data from one backend to another.
// Returns:
// 1. Status:200
// 2. Status:204
// 3. Status:500
func ShardMigrateHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		shardMigrateHandler(proxy, w, r)
	}
	return f
}

func shardMigrateHandler(proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	log := shiftlog.NewStdLog(shiftlog.Level(shiftlog.INFO))
	p := &migrateParams{
		ToFlavor:               shift.ToMySQLFlavor,
		RadonURL:               "http://" + proxy.Config().Proxy.PeerAddress,
		Cleanup:                false,
		MySQLDump:              "mysqldump",
		Threads:                16,
		Behinds:                2048,
		Checksum:               true,
		WaitTimeBeforeChecksum: 10,
	}
	err := r.DecodeJsonPayload(&p)
	if err != nil {
		log.Error("api.v1.shard.migrate.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// check args.
	if len(p.From) == 0 || len(p.FromUser) == 0 || len(p.FromDatabase) == 0 || len(p.FromTable) == 0 ||
		len(p.To) == 0 || len(p.ToUser) == 0 || len(p.ToDatabase) == 0 || len(p.ToTable) == 0 {
		log.Error("api.v1.shard.migrate[%+v].error:some param is empty", p)
		rest.Error(w, "some args are empty", http.StatusNoContent)
		return
	}
	log.Warning(`
           IMPORTANT: Please check that the shift run completes successfully.
           At the end of a successful shift run prints "shift.completed.OK!".`)

	cfg := &shift.Config{
		ToFlavor:               p.ToFlavor,
		From:                   p.From,
		FromUser:               p.FromUser,
		FromPassword:           p.FromPassword,
		FromDatabase:           p.FromDatabase,
		FromTable:              p.FromTable,
		To:                     p.To,
		ToUser:                 p.ToUser,
		ToPassword:             p.ToPassword,
		ToDatabase:             p.ToDatabase,
		ToTable:                p.ToTable,
		Cleanup:                p.Cleanup,
		MySQLDump:              p.MySQLDump,
		Threads:                p.Threads,
		Behinds:                p.Behinds,
		RadonURL:               p.RadonURL,
		Checksum:               p.Checksum,
		WaitTimeBeforeChecksum: p.WaitTimeBeforeChecksum,
	}
	log.Info("shift.cfg:%+v", cfg)

	shift := shift.NewShift(log, cfg)
	if err := shift.Start(); err != nil {
		log.Error("shift.start.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = shift.WaitFinish()
	if err != nil {
		log.Error("shift.wait.finish.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Warning("api.v1.shard.migrate.done...")
}
