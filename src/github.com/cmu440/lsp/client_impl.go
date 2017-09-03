// Contains the implementation of a LSP client.

package lsp

import (
	"bytes"
	"encoding/json"
	"errors"
	"time"

	"github.com/cmu440/lspnet"
)

type client struct {
	params     *Params
	conn       *lspnet.UDPConn
	connID     int
	seqNum     int
	epochCount int

	closed  bool
	close   chan bool
	connAck chan struct{}
	quit    chan struct{}

	read      *UChannel
	reqSeqNum chan int
	incoming  chan *Message
	outgoing  chan *Message

	rWindow *ReceiveWindow
	sWindow *SendWindow
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
	c := &client{
		params:     params,
		conn:       nil,
		connID:     0,
		seqNum:     1,
		epochCount: 0,

		closed:  false,
		close:   make(chan bool),
		connAck: make(chan struct{}),
		quit:    make(chan struct{}),

		read:      NewUnboundedChannel(),
		reqSeqNum: make(chan int),
		incoming:  make(chan *Message),
		outgoing:  make(chan *Message),

		rWindow: NewReceiveWindow(params.WindowSize),
		sWindow: NewSendWindow(params.WindowSize),
	}

	addr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	conn, err := lspnet.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, err
	}
	c.conn = conn

	go c.run()
	go c.handleConn()

	c.writeMsg(NewConnect())
	<-c.connAck

	return c, nil
}

func (c *client) ConnID() int {
	return c.connID
}

func (c *client) Read() ([]byte, error) {
	select {
	case m := <-c.read.out:
		switch m.Type {
		case MsgAck:
			return nil, errors.New("Client has been closed")
		case MsgData:
			return m.Payload, nil
		}
	case <-c.quit:
	}
	return nil, errors.New("Client has been disconnected")
}

func (c *client) Write(payload []byte) error {
	c.reqSeqNum <- 0
	seqNum := <-c.reqSeqNum
	if seqNum > 0 {
		c.outgoing <- NewData(c.connID, seqNum, payload, hash(c.connID, seqNum, payload))
		return nil
	}

	return errors.New("Client has been disconnected")
}

func (c *client) Close() error {
	c.close <- true
	return nil
}

func (c *client) readMsg(b []byte) error {
	var m Message
	err := json.Unmarshal(b, &m)
	if err != nil {
		return err
	}

	c.incoming <- &m

	return nil
}

func (c *client) writeMsg(m *Message) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}

	_, err = c.conn.Write(b)
	if err != nil {
		return err
	}

	return nil
}

func (c *client) run() {
	ticker := time.NewTicker(time.Duration(c.params.EpochMillis) * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			c.handleEpoch()
		case <-c.reqSeqNum:
			c.handleSeqNum()
		case m := <-c.incoming:
			c.handleIncoming(m)
		case m := <-c.outgoing:
			c.handleOutgoing(m)
		case <-c.close:
			c.handleClose()
		case <-c.quit:
			return
		}
	}
}

func (c *client) handleConn() {
	for {
		select {
		case <-c.quit:
			return
		default:
			var b [2000]byte
			n, err := c.conn.Read(b[:])
			if err != nil {
				if c.closed {
					return
				}
				continue
			}
			c.readMsg(b[:n])
		}
	}
}

func (c *client) handleEpoch() {
	c.epochCount++
	if (c.epochCount > c.params.EpochLimit) || (c.closed && len(c.sWindow.window) == 0) {
		c.conn.Close()
		close(c.quit)
		return
	}
	if c.connID == 0 {
		c.writeMsg(NewConnect())
	} else {
		c.writeMsg(NewAck(c.connID, 0))
		for i := c.sWindow.base; i < c.sWindow.base+c.sWindow.size; i++ {
			if m, ok := c.sWindow.window[i]; ok {
				c.writeMsg(m)
			}
		}
	}
}

func (c *client) handleSeqNum() {
	if !c.closed {
		c.reqSeqNum <- c.seqNum
		c.seqNum++
	} else {
		c.reqSeqNum <- 0
	}
}

func (c *client) handleIncoming(m *Message) {
	c.epochCount = 0
	switch m.Type {
	case MsgData:
		c.handleData(m)
	case MsgAck:
		c.handleAck(m)
	}
}

func (c *client) handleData(m *Message) {
	c.writeMsg(NewAck(c.connID, m.SeqNum))
	if !bytes.Equal(hash(m.ConnID, m.SeqNum, m.Payload), m.Hash) || !c.rWindow.inWindow(m) {
		return
	}
	c.rWindow.receive(m)
	for {
		if m, ok := c.rWindow.read(); ok {
			c.read.in <- m
		} else {
			break
		}
	}
}

func (c *client) handleAck(m *Message) {
	if m.SeqNum == 0 && c.connID == 0 {
		c.connID = m.ConnID
		close(c.connAck)
	} else if m.SeqNum > 0 {
		i := c.sWindow.base + c.sWindow.size
		c.sWindow.ack(m)
		for ; i < c.sWindow.base+c.sWindow.size; i++ {
			if m, ok := c.sWindow.window[i]; ok {
				c.writeMsg(m)
			}
		}
	}
}

func (c *client) handleOutgoing(m *Message) {
	c.sWindow.send(m)
	if c.sWindow.inWindow(m) {
		c.writeMsg(m)
	}
}

func (c *client) handleClose() {
	c.closed = true
	c.read.in <- NewAck(c.connID, 0)
	c.read.Close()
}
