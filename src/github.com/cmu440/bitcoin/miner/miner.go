package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
)

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./miner <hostport>")
		return
	}

	m, err := NewMiner(os.Args[1], lsp.NewParams())
	if err != nil {
		fmt.Println("Invalid hostport:", os.Args[1])
		return
	}

	m.run()
}

type Miner struct {
	lspClient lsp.Client

	jobs chan *bitcoin.Message
	quit chan struct{}
}

func NewMiner(hostport string, params *lsp.Params) (*Miner, error) {
	lspClient, err := lsp.NewClient(hostport, params)
	if err != nil {
		return nil, err
	}

	m := &Miner{
		lspClient: lspClient,

		jobs: make(chan *bitcoin.Message),
		quit: make(chan struct{}),
	}

	return m, nil
}

func (m *Miner) readMsg(b []byte) error {
	var msg bitcoin.Message
	err := json.Unmarshal(b, &msg)
	if err != nil {
		return err
	}

	if msg.Type == bitcoin.Request {
		m.jobs <- &msg
	}

	return nil
}

func (m *Miner) writeMsg(msg *bitcoin.Message) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	err = m.lspClient.Write(b)
	if err != nil {
		return err
	}

	return nil
}

func (m *Miner) handleConn() {
	for {
		b, err := m.lspClient.Read()
		if err != nil {
			m.lspClient.Close()
			close(m.quit)
			return
		}
		m.readMsg(b)
	}
}

func (m *Miner) run() {
	m.writeMsg(bitcoin.NewJoin())
	go m.handleConn()
	for {
		select {
		case msg := <-m.jobs:
			hash, nonce := m.computeMinHash(msg.Data, msg.Lower, msg.Upper)
			m.writeMsg(bitcoin.NewResult(hash, nonce))
		case <-m.quit:
			return
		}
	}
}

func (m *Miner) computeMinHash(data string, lower, upper uint64) (hash, nonce uint64) {
	var minHash, minNonce uint64
	for nonce := lower; nonce <= upper; nonce++ {
		hash = bitcoin.Hash(data, nonce)
		if nonce == lower || hash < minHash {
			minHash, minNonce = hash, nonce
		}
	}
	return minHash, minNonce
}
