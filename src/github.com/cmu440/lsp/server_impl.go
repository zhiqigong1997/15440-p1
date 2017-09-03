// Contains the implementation of a LSP server.

package lsp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/cmu440/lspnet"
)

type server struct {
	params *Params
	conn   *lspnet.UDPConn
	connID int

	clients map[int]*serverClient
	addrs   map[string]int

	closed    bool
	closeConn chan int
	close     chan bool
	quit      chan struct{}

	connecting chan *lspnet.UDPAddr
	read       *UChannel
	reqSeqNum  chan int
	resSeqNum  chan int
	incoming   chan *Message
	outgoing   chan *Message
}

type serverClient struct {
	addr       *lspnet.UDPAddr
	connID     int
	seqNum     int
	epochCount int

	closed bool

	rWindow *ReceiveWindow
	sWindow *SendWindow
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	s := &server{
		params: params,
		conn:   nil,
		connID: 1,

		clients: make(map[int]*serverClient),
		addrs:   make(map[string]int),

		closed:    false,
		closeConn: make(chan int),
		close:     make(chan bool),
		quit:      make(chan struct{}),

		connecting: make(chan *lspnet.UDPAddr),
		read:       NewUnboundedChannel(),
		reqSeqNum:  make(chan int),
		resSeqNum:  make(chan int),
		incoming:   make(chan *Message),
		outgoing:   make(chan *Message),
	}

	addr, err := lspnet.ResolveUDPAddr("udp", lspnet.JoinHostPort("localhost", strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}
	conn, err := lspnet.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	s.conn = conn

	go s.run()
	go s.handleConn()

	return s, nil
}

func (s *server) Read() (int, []byte, error) {
	select {
	case m := <-s.read.out:
		switch m.Type {
		case MsgConnect:
			return m.ConnID, nil, fmt.Errorf("Client %d has been disconnected", m.ConnID)
		case MsgAck:
			return m.ConnID, nil, fmt.Errorf("Client %d connection has been closed", m.ConnID)
		case MsgData:
			return m.ConnID, m.Payload, nil
		}
	case <-s.quit:
	}
	return 0, nil, errors.New("Server has been closed")
}

func (s *server) Write(connID int, payload []byte) error {
	s.reqSeqNum <- connID
	seqNum := <-s.resSeqNum
	if seqNum > 0 {
		s.outgoing <- NewData(connID, seqNum, payload, hash(connID, seqNum, payload))
		return nil
	}

	return fmt.Errorf("Client %d does not exists", connID)
}

func (s *server) CloseConn(connID int) error {
	s.closeConn <- connID
	s.read.in <- NewAck(connID, 0)
	return nil
}

func (s *server) Close() error {
	s.close <- true
	<-s.quit
	return nil
}

func NewServerClient(connID int, addr *lspnet.UDPAddr, params *Params) *serverClient {
	c := &serverClient{
		addr:       addr,
		connID:     connID,
		seqNum:     1,
		epochCount: 0,

		closed: false,

		rWindow: NewReceiveWindow(params.WindowSize),
		sWindow: NewSendWindow(params.WindowSize),
	}
	return c
}

func (s *server) readMsg(b []byte, addr *lspnet.UDPAddr) error {
	var m Message
	err := json.Unmarshal(b, &m)
	if err != nil {
		return err
	}

	switch m.Type {
	case MsgConnect:
		s.connecting <- addr
	default:
		s.incoming <- &m
	}

	return nil
}

func (s *server) writeMsg(m *Message) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}

	_, err = s.conn.WriteToUDP(b, s.clients[m.ConnID].addr)
	if err != nil {
		return err
	}

	return nil
}

func (s *server) disconnectConn(connID int) {
	if c, ok := s.clients[connID]; ok {
		delete(s.clients, connID)
		delete(s.addrs, c.addr.String())
	}
}

func (s *server) run() {
	ticker := time.NewTicker(time.Duration(s.params.EpochMillis) * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			s.handleEpoch()
		case m := <-s.connecting:
			s.handleConnecting(m)
		case connID := <-s.reqSeqNum:
			s.handleSeqNum(connID)
		case m := <-s.incoming:
			s.handleIncoming(m)
		case m := <-s.outgoing:
			s.handleOutgoing(m)
		case connID := <-s.closeConn:
			s.handleCloseConn(connID)
		case <-s.close:
			s.handleClose()
		case <-s.quit:
			return
		}
	}
}

func (s *server) handleConn() {
	for {
		select {
		case <-s.quit:
			return
		default:
			var b [2000]byte
			n, addr, err := s.conn.ReadFromUDP(b[:])
			if err != nil {
				return
			}
			s.readMsg(b[:n], addr)
		}
	}
}

func (s *server) handleEpoch() {
	if s.closed && len(s.clients) == 0 {
		s.conn.Close()
		close(s.quit)
		return
	}
	for _, c := range s.clients {
		c.epochCount++
		if c.epochCount > s.params.EpochLimit {
			m := NewConnect()
			m.ConnID = c.connID
			s.read.in <- m
			s.disconnectConn(c.connID)
		} else if c.closed && len(c.sWindow.window) == 0 {
			s.disconnectConn(c.connID)
		} else {
			s.writeMsg(NewAck(c.connID, 0))
			for i := c.sWindow.base; i < c.sWindow.base+c.sWindow.size; i++ {
				if m, ok := c.sWindow.window[i]; ok {
					s.writeMsg(m)
				}
			}
		}
	}
}

func (s *server) handleConnecting(addr *lspnet.UDPAddr) {
	if _, ok := s.addrs[addr.String()]; !ok {
		s.addrs[addr.String()] = s.connID
		s.clients[s.connID] = NewServerClient(s.connID, addr, s.params)
		s.connID++
	}
	s.writeMsg(NewAck(s.addrs[addr.String()], 0))
}

func (s *server) handleSeqNum(connID int) {
	if c, ok := s.clients[connID]; ok {
		s.resSeqNum <- c.seqNum
		c.seqNum++
	} else {
		s.resSeqNum <- 0
	}
}

func (s *server) handleIncoming(m *Message) {
	if c, ok := s.clients[m.ConnID]; ok {
		c.epochCount = 0
	}
	switch m.Type {
	case MsgData:
		s.handleData(m)
	case MsgAck:
		s.handleAck(m)
	}
}

func (s *server) handleData(m *Message) {
	c := s.clients[m.ConnID]
	if !bytes.Equal(hash(m.ConnID, m.SeqNum, m.Payload), m.Hash) || !c.rWindow.inWindow(m) {
		return
	}
	c.rWindow.receive(m)
	s.writeMsg(NewAck(c.connID, m.SeqNum))
	for {
		if m, ok := c.rWindow.read(); ok {
			s.read.in <- m
		} else {
			break
		}
	}
}

func (s *server) handleAck(m *Message) {
	if m.SeqNum > 0 {
		c := s.clients[m.ConnID]
		i := c.sWindow.base + c.sWindow.size
		c.sWindow.ack(m)
		for ; i < c.sWindow.base+c.sWindow.size; i++ {
			if m, ok := c.sWindow.window[i]; ok {
				s.writeMsg(m)
			}
		}
	}
}

func (s *server) handleOutgoing(m *Message) {
	c := s.clients[m.ConnID]
	c.sWindow.send(m)
	if c.sWindow.inWindow(m) {
		s.writeMsg(m)
	}
}

func (s *server) handleCloseConn(connID int) {
	s.clients[connID].closed = true
}

func (s *server) handleClose() {
	s.closed = true
	for connID := range s.clients {
		s.handleCloseConn(connID)
	}
	s.read.Close()
}
