package tcp

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"
)

const (
	DefaultWriterBufferSize = 1 << 10
	DefaultProtocol         = "tcp"
	RetryDelay              = 10 * time.Millisecond
)

type RequestInfo struct {
	Data       []byte
	Connection *Connection
}

type ResponseInfo struct {
	Data  any
	Ready context.CancelFunc
	Ctx   context.Context
}

type Connection struct {
	mu      sync.Mutex
	address string

	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer

	done     chan struct{}
	readerCh chan RequestInfo
	writerCh chan any
}

func NewConnection(conn net.Conn, requests chan RequestInfo) *Connection {
	connection := &Connection{
		address:  conn.RemoteAddr().String(),
		conn:     conn,
		reader:   bufio.NewReader(conn),
		writer:   bufio.NewWriter(conn),
		writerCh: make(chan any, DefaultWriterBufferSize),
		readerCh: requests,
		done:     make(chan struct{}),
	}

	go connection.writerLoop()
	go connection.readerLoop()

	return connection
}

func (connection *Connection) Reconnect() error {
	if connection.isClosed() {
		return io.ErrClosedPipe
	}

	conn, err := net.Dial(DefaultProtocol, connection.address)
	if err != nil {
		return err
	}

	connection.mu.Lock()
	defer connection.mu.Unlock()

	if connection.conn != nil {
		_ = conn.Close()
		return nil
	}

	connection.conn = conn
	connection.reader = bufio.NewReader(conn)
	connection.writer = bufio.NewWriter(conn)

	return nil
}

func (connection *Connection) Send(msg any) error {
	select {
	case <-connection.done:
		return io.ErrClosedPipe
	case connection.writerCh <- msg:
		return nil
	}
}

func (connection *Connection) Close() {
	select {
	case <-connection.done:
		return
	default:
		close(connection.done)
	}

	connection.dropConn()
}

func (connection *Connection) dropConn() {
	connection.mu.Lock()
	defer connection.mu.Unlock()

	if connection.conn != nil {
		_ = connection.conn.Close()
	}

	connection.conn = nil
	connection.reader = nil
	connection.writer = nil
}

func (connection *Connection) getReader() *bufio.Reader {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	return connection.reader
}

func (connection *Connection) getWriter() *bufio.Writer {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	return connection.writer
}

func (connection *Connection) writerLoop() {
	var pending any

	for {
		if connection.isClosed() {
			return
		}

		if pending == nil {
			select {
			case <-connection.done:
				return
			case pending = <-connection.writerCh:
			}
		}

		writer := connection.getWriter()
		if writer == nil {
			time.Sleep(RetryDelay)
			continue
		}

		if err := json.NewEncoder(writer).Encode(pending); err != nil {
			slog.Warn("Write failed", "remote", connection.address, "error", err)
			connection.dropConn()
			continue
		}

		if err := writer.Flush(); err != nil {
			slog.Warn("Flush failed", "remote", connection.address, "error", err)
			connection.dropConn()
			continue
		}

		pending = nil
	}
}

func (connection *Connection) readerLoop() {
	for {
		if connection.isClosed() {
			return
		}

		reader := connection.getReader()
		if reader == nil {
			time.Sleep(RetryDelay)
			continue
		}

		line, err := reader.ReadBytes('\n')
		if err != nil {
			if connection.isClosed() {
				return
			}

			if err != io.EOF {
				slog.Warn("Request read failed", "remote", connection.address, "error", err)
			}

			connection.dropConn()
			continue
		}

		select {
		case <-connection.done:
			return
		case connection.readerCh <- RequestInfo{
			Data:       line,
			Connection: connection,
		}:
		}
	}
}
func (connection *Connection) isClosed() bool {
	select {
	case <-connection.done:
		return true
	default:
		return false
	}
}
