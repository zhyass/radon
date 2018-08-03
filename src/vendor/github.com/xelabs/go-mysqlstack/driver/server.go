/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package driver

import (
	"io"
	"net"
	"runtime"
	"runtime/debug"

	"monitor"

	"github.com/xelabs/go-mysqlstack/common"
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// Handler interface.
type Handler interface {
	// NewSession is called when a session is coming.
	NewSession(session *Session)

	// SessionClosed is called when a session exit.
	SessionClosed(session *Session)

	// Check the session.
	SessionCheck(session *Session) error

	// Check the Auth request.
	AuthCheck(session *Session) error

	// Handle the cominitdb.
	ComInitDB(session *Session, database string) error

	// Handle the queries.
	ComQuery(session *Session, query string, callback func(*sqltypes.Result) error) error
}

// Listener is a connection handler.
type Listener struct {
	// Logger.
	log *xlog.Log

	address string

	// Query handler.
	handler Handler

	// This is the main listener socket.
	listener net.Listener

	// Incrementing ID for connection id.
	connectionID uint32
}

// NewListener creates a new Listener.
func NewListener(log *xlog.Log, address string, handler Handler) (*Listener, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	return &Listener{
		log:          log,
		address:      address,
		handler:      handler,
		listener:     listener,
		connectionID: 1,
	}, nil
}

// Accept runs an accept loop until the listener is closed.
func (l *Listener) Accept() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	for {
		conn, err := l.listener.Accept()
		if err != nil {
			// Close() was probably called.
			return
		}
		ID := l.connectionID
		l.connectionID++
		go l.handle(conn, ID)
	}
}

func (l *Listener) parserComInitDB(data []byte) string {
	return string(data[1:])
}

func (l *Listener) parserComQuery(data []byte) string {
	// Trim the right.
	data = data[1:]
	last := len(data) - 1
	if data[last] == ';' {
		data = data[:last]
	}
	return common.BytesToString(data)
}

// handle is called in a go routine for each client connection.
func (l *Listener) handle(conn net.Conn, ID uint32) {
	var err error
	var data []byte
	var authPkt []byte
	var greetingPkt []byte
	log := l.log

	// Catch panics, and close the connection in any case.
	defer func() {
		conn.Close()
		if x := recover(); x != nil {
			log.Error("server.handle.panic:\n%v\n%s", x, debug.Stack())
		}
	}()
	session := newSession(log, ID, conn)
	// Session check.
	if err = l.handler.SessionCheck(session); err != nil {
		log.Warning("session[%v].check.failed.error:%+v", ID, err)
		session.writeErrFromError(err)
		return
	}

	// Session register.
	l.handler.NewSession(session)
	defer l.handler.SessionClosed(session)

	// Greeting packet.
	greetingPkt = session.greeting.Pack()
	if err = session.packets.Write(greetingPkt); err != nil {
		log.Error("server.write.greeting.packet.error: %v", err)
		return
	}

	// Auth packet.
	if authPkt, err = session.packets.Next(); err != nil {
		log.Error("server.read.auth.packet.error: %v", err)
		return
	}
	if err = session.auth.UnPack(authPkt); err != nil {
		log.Error("server.unpack.auth.error: %v", err)
		return
	}

	//  Auth check.
	if err = l.handler.AuthCheck(session); err != nil {
		log.Warning("server.user[%+v].auth.check.failed", session.User())
		session.writeErrFromError(err)
		return
	}

	// Check the database.
	db := session.auth.Database()
	if db != "" {
		if err = l.handler.ComInitDB(session, db); err != nil {
			log.Error("server.cominitdb[%s].error:%+v", db, err)
			session.writeErrFromError(err)
			return
		}
		session.SetSchema(db)
	}

	if err = session.packets.WriteOK(0, 0, session.greeting.Status(), 0); err != nil {
		return
	}

	monitor.ClientConnectionInc(session.User())
	defer monitor.ClientConnectionDec(session.User())
	for {
		// Reset packet sequence ID.
		session.packets.ResetSeq()
		if data, err = session.packets.Next(); err != nil {
			return
		}

		isSuccess := false
		if isSuccess, err = l.handleData(data, session); err != nil {
			if err == io.EOF {
				l.queryStat(data[0], true)
			} else {
				l.queryStat(data[0], false)
			}
			return
		}
		l.queryStat(data[0], isSuccess)

		// Reset packet sequence ID.
		session.packets.ResetSeq()
	}
}

// Addr returns the client address.
func (l *Listener) Addr() string {
	return l.address
}

// Close close the listener and all connections.
func (l *Listener) Close() {
	l.listener.Close()
}

//if the sql handle success return true, else return false.
func (l *Listener) handleData(data []byte, s *Session) (bool, error) {
	var err error
	switch data[0] {
	case sqldb.COM_QUIT:
		return true, io.EOF
	case sqldb.COM_INIT_DB:
		db := l.parserComInitDB(data)
		if err = l.handler.ComInitDB(s, db); err != nil {
			return false, s.writeErrFromError(err)
		}
		s.SetSchema(db)
		return true, s.packets.WriteOK(0, 0, s.greeting.Status(), 0)
	case sqldb.COM_PING:
		return true, s.packets.WriteOK(0, 0, s.greeting.Status(), 0)
	case sqldb.COM_QUERY:
		query := l.parserComQuery(data)
		if err = l.handler.ComQuery(s, query, func(qr *sqltypes.Result) error {
			return s.writeResult(qr)
		}); err != nil {
			s.log.Error("server.handle.query.from.session[%v].error:%+v.query[%s]", s.id, err, query)
			return false, s.writeErrFromError(err)
		}
	default:
		cmd := sqldb.CommandString(data[0])
		s.log.Error("session.command:%s.not.implemented", cmd)
		sqlErr := sqldb.NewSQLError(sqldb.ER_UNKNOWN_ERROR, "command handling not implemented yet: %s", cmd)
		return false, s.writeErrFromError(sqlErr)
	}
	return true, nil
}

func (l *Listener) queryStat(cmd byte, isSuccess bool) {
	var command string
	switch cmd {
	case sqldb.COM_QUIT:
		command = "Quit"
	case sqldb.COM_INIT_DB:
		command = "InitDB"
	case sqldb.COM_PING:
		command = "Ping"
	case sqldb.COM_QUERY:
		command = "Query"
	default:
		command = "Unsupport"
	}
	if isSuccess {
		monitor.QueryTotalCounterInc(command, "OK")
	} else {
		monitor.QueryTotalCounterInc(command, "Error")
	}
}
