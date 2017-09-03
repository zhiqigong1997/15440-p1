package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
)

func main() {
	const numArgs = 4
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./client <hostport> <message> <maxNonce>")
		return
	}
	maxNonce, err := strconv.ParseUint(os.Args[3], 10, 64)
	if err != nil {
		fmt.Println("Invalid maxNonce:", os.Args[3])
		return
	}

	c, err := lsp.NewClient(os.Args[1], lsp.NewParams())
	if err != nil {
		fmt.Println("Invalid hostport:", os.Args[1])
		return
	}

	b, err := json.Marshal(bitcoin.NewRequest(os.Args[2], 0, maxNonce))
	if err != nil {
		fmt.Println("Error sending request")
		return
	}
	err = c.Write(b)
	if err != nil {
		printDisconnected()
		return
	}

	b, err = c.Read()
	if err != nil {
		printDisconnected()
		return
	}
	var m bitcoin.Message
	err = json.Unmarshal(b, &m)
	if err != nil {
		fmt.Println("Error receiving result")
		return
	}

	printResult(strconv.FormatUint(m.Hash, 10), strconv.FormatUint(m.Nonce, 10))
}

// printResult prints the final result to stdout.
func printResult(hash, nonce string) {
	fmt.Println("Result", hash, nonce)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	fmt.Println("Disconnected")
}
