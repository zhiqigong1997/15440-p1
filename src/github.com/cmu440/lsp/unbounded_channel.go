package lsp

import (
	"container/list"
)

type UChannel struct {
	buf *list.List
	in  chan *Message
	out chan *Message
}

func NewUnboundedChannel() *UChannel {
	u := &UChannel{
		buf: list.New(),
		in:  make(chan *Message),
		out: make(chan *Message),
	}
	go u.run()
	return u
}

func (u *UChannel) Close() {
	close(u.in)
}

func (u *UChannel) run() {
	defer close(u.out)
	for {
		if u.buf.Len() == 0 {
			m, ok := <-u.in
			if !ok {
				u.flush()
				return
			}
			u.buf.PushBack(m)
		}

		select {
		case m, ok := <-u.in:
			if !ok {
				u.flush()
				return
			}
			u.buf.PushBack(m)
		case u.out <- (u.buf.Front().Value).(*Message):
			u.buf.Remove(u.buf.Front())
		}
	}
}

func (u *UChannel) flush() {
	for e := u.buf.Front(); e != nil; e = e.Next() {
		u.out <- (e.Value).(*Message)
	}
}
