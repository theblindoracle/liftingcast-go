package liftingcast

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"time"
)

// Replayer replays a recording, see ADR-0004: it feeds the recording's meet
// state messages through a Cache in order, as the Hub does live, and returns
// the resulting Updates with each message's original connection number and
// receive time.
//
// The Replayer skips what it does not use: events other than meet state, and
// unknown events and fields. A final line cut short by a crash is skipped,
// and a recording with no closed or replaced event is not an error. A message
// the Cache rejects is skipped, as the Hub skips it. Any other line that is
// not valid JSON is an error.
//
// A Replayer is not safe for concurrent use.
type Replayer struct {
	r     *bufio.Reader
	cache *Cache
	line  int
	err   error
}

// NewReplayer returns a Replayer for the recording read from r, in the JSONL
// format WithRecorder writes (and cmd/lc-tap and testdata/ use).
func NewReplayer(r io.Reader) *Replayer {
	return &Replayer{r: bufio.NewReader(r), cache: NewCache()}
}

// Next returns the next meet state of the recording, or io.EOF once there
// are no more. After any other error the Replayer returns that error again.
func (p *Replayer) Next() (Update, error) {
	for p.err == nil {
		line, err := p.r.ReadBytes('\n')
		if err != nil && err != io.EOF {
			p.err = err
			break
		}
		atEOF := err == io.EOF
		p.line++

		if u, ok, err := p.decode(line, atEOF); err != nil {
			p.err = err
		} else if ok {
			return u, nil
		}
		if atEOF {
			p.err = io.EOF
		}
	}
	return Update{}, p.err
}

// decode handles one line. ok is false if the line holds no meet state.
func (p *Replayer) decode(line []byte, last bool) (u Update, ok bool, err error) {
	var e struct {
		Rx   string          `json:"rx"`
		Conn uint64          `json:"conn"`
		Msg  json.RawMessage `json:"msg"`
	}
	if err := json.Unmarshal(line, &e); err != nil {
		// Only blank lines and the last line, if the crash cut it short
		// (it has no newline), are not errors
		if last || len(trimSpace(line)) == 0 {
			return Update{}, false, nil
		}
		return Update{}, false, fmt.Errorf("liftingcast: replay line %d: %w", p.line, err)
	}
	if len(e.Msg) == 0 {
		return Update{}, false, nil
	}
	state, err := p.cache.Merge(e.Msg)
	if err != nil {
		return Update{}, false, nil
	}
	var at time.Time
	if e.Rx != "" {
		at, _ = time.Parse(time.RFC3339Nano, e.Rx)
	}
	return Update{State: state, Connection: e.Conn, ReceivedAt: at}, true, nil
}

func trimSpace(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t' || b[0] == '\r' || b[0] == '\n') {
		b = b[1:]
	}
	return b
}

// All returns an iterator over the rest of the recording's meet states. It
// yields a non-nil error once, if the recording cannot be read, and then
// stops.
func (p *Replayer) All() iter.Seq2[Update, error] {
	return func(yield func(Update, error) bool) {
		for {
			u, err := p.Next()
			if err == io.EOF {
				return
			}
			if !yield(u, err) || err != nil {
				return
			}
		}
	}
}
