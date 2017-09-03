package main

import (
	"container/list"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
)

const (
	DefaultMaxJobSize = uint64(10000)
)

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./server <port>")
		return
	}
	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Invalid port:", os.Args[1])
		return
	}

	s, err := NewServer(port, lsp.NewParams(), DefaultMaxJobSize)
	if err != nil {
		fmt.Println("Invalid port:", os.Args[1])
		return
	}
	s.run()
}

type Server struct {
	lspServer lsp.Server

	maxJobSize uint64

	incoming      chan *ServerMessage
	requests      chan *ServerMessage
	results       chan *ServerMessage
	disconnecting chan int

	clients map[int]chan *bitcoin.Message
	miners  map[int]*ServerMessage

	pendingRequests *list.List
	availableMiners *list.List
}

type ServerMessage struct {
	connID  int
	message *bitcoin.Message
}

func NewServer(port int, params *lsp.Params, maxJobSize uint64) (*Server, error) {
	lspServer, err := lsp.NewServer(port, params)
	if err != nil {
		return nil, err
	}

	s := &Server{
		lspServer: lspServer,

		maxJobSize: maxJobSize,

		incoming:      make(chan *ServerMessage),
		requests:      make(chan *ServerMessage),
		results:       make(chan *ServerMessage),
		disconnecting: make(chan int),

		clients: make(map[int]chan *bitcoin.Message),
		miners:  make(map[int]*ServerMessage),

		pendingRequests: list.New(),
		availableMiners: list.New(),
	}

	return s, nil
}

func NewServerMessage(connID int, m *bitcoin.Message) *ServerMessage {
	return &ServerMessage{
		connID:  connID,
		message: m,
	}
}

func (s *Server) readMsg(connID int, b []byte) error {
	var m bitcoin.Message
	err := json.Unmarshal(b, &m)
	if err != nil {
		return err
	}

	s.incoming <- NewServerMessage(connID, &m)

	return nil
}

func (s *Server) writeMsg(connID int, m *bitcoin.Message) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}

	err = s.lspServer.Write(connID, b)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) sendMinerRequest(sm *ServerMessage) {
	s.requests <- sm
}

func (s *Server) handleConn() {
	for {
		connID, b, err := s.lspServer.Read()
		if err != nil {
			s.disconnecting <- connID
		}
		s.readMsg(connID, b)
	}
}

func (s *Server) run() {
	go s.handleConn()
	for {
		select {
		case sm := <-s.incoming:
			s.handleIncoming(sm)
		case sm := <-s.requests:
			s.handleMinerRequest(sm)
		case sm := <-s.results:
			s.handleClientResult(sm)
		case connID := <-s.disconnecting:
			s.handleDisconnect(connID)
		}
	}
}

func (s *Server) handleIncoming(sm *ServerMessage) {
	switch sm.message.Type {
	case bitcoin.Request:
		s.handleClientRequest(sm.connID, sm.message)
	case bitcoin.Result:
		s.handleMinerResult(sm.connID, sm.message)
	case bitcoin.Join:
		s.handleJoin(sm.connID)
	}
}

func (s *Server) handleClientRequest(connID int, m *bitcoin.Message) {
	results := make(chan *bitcoin.Message)
	s.clients[connID] = results
	go s.handleClient(connID, m.Data, m.Lower, m.Upper, results)
}

func (s *Server) handleClient(connID int, data string, lower, upper uint64, results chan *bitcoin.Message) {
	for nonce := lower; nonce <= upper; nonce += s.maxJobSize {
		go s.sendMinerRequest(NewServerMessage(connID, bitcoin.NewRequest(data, nonce, min(nonce+s.maxJobSize, upper))))
	}
	var minHash, minNonce uint64
	for nonce := lower; nonce <= upper; nonce += s.maxJobSize {
		if m, ok := <-results; !ok {
			return
		} else if nonce == lower || m.Hash < minHash {
			minHash, minNonce = m.Hash, m.Nonce
		}
	}
	s.results <- NewServerMessage(connID, bitcoin.NewResult(minHash, minNonce))
}

func (s *Server) handleMinerResult(connID int, m *bitcoin.Message) {
	if results, ok := s.clients[s.miners[connID].connID]; ok {
		results <- m
	}
	s.handleMinerAvailable(connID)
}

func (s *Server) handleJoin(connID int) {
	s.handleMinerAvailable(connID)
}

func (s *Server) handleMinerAvailable(connID int) {
	for s.pendingRequests.Len() > 0 {
		sm := s.pendingRequests.Remove(s.pendingRequests.Front()).(*ServerMessage)
		if _, ok := s.clients[sm.connID]; ok {
			s.miners[connID] = sm
			s.writeMsg(connID, sm.message)
			return
		}
	}
	s.miners[connID] = nil
	s.availableMiners.PushBack(connID)
}

func (s *Server) handleMinerRequest(sm *ServerMessage) {
	for s.availableMiners.Len() > 0 {
		connID := s.availableMiners.Remove(s.availableMiners.Front()).(int)
		if _, ok := s.miners[connID]; ok {
			s.miners[connID] = sm
			s.writeMsg(connID, sm.message)
			return
		}
	}
	s.pendingRequests.PushBack(sm)
}

func (s *Server) handleClientResult(sm *ServerMessage) {
	s.writeMsg(sm.connID, sm.message)
	delete(s.clients, sm.connID)
	s.lspServer.CloseConn(sm.connID)
}

func (s *Server) handleDisconnect(connID int) {
	if ch, ok := s.clients[connID]; ok {
		delete(s.clients, connID)
		close(ch)
	} else if sm, ok := s.miners[connID]; ok {
		delete(s.miners, connID)
		if sm != nil {
			go s.sendMinerRequest(sm)
		}
	}
}

func min(x, y uint64) uint64 {
	if x <= y {
		return x
	}
	return y
}
