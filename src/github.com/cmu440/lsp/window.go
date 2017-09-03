package lsp

type SendWindow struct {
	window map[int]*Message
	base   int
	size   int
}

type ReceiveWindow struct {
	window   map[int]*Message
	base     int
	size     int
	readNext int
}

func NewSendWindow(size int) *SendWindow {
	return &SendWindow{
		window: make(map[int]*Message),
		base:   1,
		size:   size,
	}
}

func NewReceiveWindow(size int) *ReceiveWindow {
	return &ReceiveWindow{
		window:   make(map[int]*Message),
		base:     1,
		size:     size,
		readNext: 1,
	}
}

func (s *SendWindow) inWindow(m *Message) bool {
	return m.SeqNum >= s.base && m.SeqNum < s.base+s.size
}

func (s *SendWindow) send(m *Message) {
	s.window[m.SeqNum] = m
}

func (s *SendWindow) ack(m *Message) {
	delete(s.window, m.SeqNum)
	for ; m.SeqNum == s.base || len(s.window) > 0; s.base++ {
		if _, ok := s.window[s.base]; ok {
			break
		}
	}
}

func (r *ReceiveWindow) inWindow(m *Message) bool {
	return m.SeqNum < r.base+r.size
}

func (r *ReceiveWindow) receive(m *Message) {
	r.window[m.SeqNum] = m
	for ; ; r.base++ {
		if _, ok := r.window[r.base]; !ok {
			break
		}
	}
}

func (r *ReceiveWindow) read() (m *Message, ok bool) {
	if m, ok := r.window[r.readNext]; ok {
		delete(r.window, r.readNext)
		r.readNext++
		return m, true
	}
	return nil, false
}
