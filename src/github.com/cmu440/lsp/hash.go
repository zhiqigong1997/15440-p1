package lsp

import (
	"crypto/md5"
	"strconv"
)

func hash(connID int, seqNum int, payload []byte) []byte {
	hash := md5.New()
	hash.Write([]byte(strconv.Itoa(connID)))
	hash.Write([]byte(strconv.Itoa(seqNum)))
	hash.Write(payload)
	return hash.Sum(nil)
}
