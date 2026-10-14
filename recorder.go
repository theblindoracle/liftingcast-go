package liftingcast

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// recorderBuffer is how many events may wait for the recording's writer
// before new ones are dropped.
const recorderBuffer = 64

// ClientInfo describes one Client to the WithRecorder factory, so it can
// choose where the recording goes. It never holds credentials.
type ClientInfo struct {
	// MeetID is the meet the Client follows.
	MeetID string
	// Sequence numbers the Clients of one run, starting at 1. A recording
	// with a higher Sequence replaced the one before it.
	Sequence uint64
	// Start is when the Client was created.
	Start time.Time
}

// RecorderFactory creates the writer for one Client's recording. The library
// closes the writer after the recording's final event.
type RecorderFactory func(ClientInfo) (io.WriteCloser, error)

// WithRecorder records each Client's upstream connection, see ADR-0004.
// newWriter is called once for each Client created, and a Client whose
// factory returns an error runs without a recording. Recording never stalls
// the live connection: events the writer cannot take are dropped, and a
// recorder_dropped event says how many.
func WithRecorder(newWriter RecorderFactory) Option {
	return func(o *options) {
		o.recorder = newWriter
	}
}

// recordEvent is one line of a recording. Its existing fields are shared
// with cmd/lc-tap; changes only add fields or events.
type recordEvent struct {
	Rx       string `json:"rx"`
	Conn     uint64 `json:"conn"`
	Event    string `json:"event,omitempty"`
	Count    int64  `json:"count,omitempty"`    // recorder_dropped
	MeetID   string `json:"meetId,omitempty"`   // start
	Sequence uint64 `json:"sequence,omitempty"` // start

	// The shapes below are those of cmd/lc-tap
	Status int             `json:"status,omitempty"` // handshake
	Code   int             `json:"code,omitempty"`   // close
	Bytes  int             `json:"bytes,omitempty"`  // meet state, text
	Msg    json.RawMessage `json:"msg,omitempty"`    // meet state
	Text   string          `json:"text,omitempty"`   // text, close
	Error  string          `json:"error,omitempty"`  // handshake, read_error, rejected
	Body   string          `json:"body,omitempty"`   // handshake

	WaitMs int64 `json:"waitMs,omitempty"` // backoff
}

// recordAt queues e as having happened at t.
func (c *Client) recordAt(t time.Time, e recordEvent) {
	if c.rec == nil {
		return
	}
	e.Rx = formatRecordTime(t)
	c.rec.record(e)
}

// recordNow queues e as having happened now.
func (c *Client) recordNow(e recordEvent) {
	c.recordAt(time.Now(), e)
}

// redact removes everything from s that must stay out of a recording: the
// credentials, the connection URL and its host. Errors from the network
// name the address they failed to reach.
func (c *Client) redact(s string) string {
	auth := base64.StdEncoding.EncodeToString([]byte(c.cfg.MeetID + ":" + c.cfg.Password))
	secrets := []string{c.cfg.Password, c.cfg.APIKey, auth, url.QueryEscape(auth), c.cfg.BaseURL}
	if u, err := url.Parse(c.cfg.BaseURL); err == nil && u.Host != "" {
		secrets = append(secrets, u.Host, u.Hostname())
	}
	// Longest first, so the URL goes before the host inside it
	slices.SortFunc(secrets, func(a, b string) int { return len(b) - len(a) })
	for _, secret := range secrets {
		if len(secret) >= 4 {
			s = strings.ReplaceAll(s, secret, "REDACTED")
		}
	}
	return s
}

// recorder writes a Client's events to its recording on its own goroutine,
// so a slow or failing writer cannot hold up the Client. A nil *recorder
// records nothing.
type recorder struct {
	events  chan recordEvent
	dropped atomic.Int64
	once    sync.Once
}

// newRecorder calls factory and starts writing the recording, beginning with
// its start event. It returns the factory's error if it fails.
func newRecorder(factory RecorderFactory, info ClientInfo) (*recorder, error) {
	w, err := factory(info)
	if err != nil {
		return nil, err
	}
	r := &recorder{events: make(chan recordEvent, recorderBuffer)}
	r.record(recordEvent{
		Rx:       formatRecordTime(info.Start),
		Event:    "start",
		MeetID:   info.MeetID,
		Sequence: info.Sequence,
	})
	go r.write(w)
	return r, nil
}

func formatRecordTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// record queues e without blocking, or counts it as dropped if the queue is
// full. It must not be called after finish.
func (r *recorder) record(e recordEvent) {
	if r == nil {
		return
	}
	select {
	case r.events <- e:
	default:
		r.dropped.Add(1)
	}
}

// finish queues the final event, then lets the writer drain and close the
// recording. It does not wait for the writer.
func (r *recorder) finish(final string) {
	if r == nil {
		return
	}
	r.once.Do(func() {
		r.record(recordEvent{Rx: formatRecordTime(time.Now()), Event: final})
		close(r.events)
	})
}

// write is the recorder's goroutine: one Write call for each line. Events
// lost to a full queue or a failed write are reported by a recorder_dropped
// event ahead of the next line that gets written.
func (r *recorder) write(w io.WriteCloser) {
	defer w.Close()
	writeLine := func(e recordEvent) bool {
		line, err := json.Marshal(e)
		if err != nil {
			return false
		}
		_, err = w.Write(append(line, '\n'))
		return err == nil
	}
	flushDropped := func() {
		if n := r.dropped.Swap(0); n > 0 {
			e := recordEvent{Rx: formatRecordTime(time.Now()), Event: "recorder_dropped", Count: n}
			if !writeLine(e) {
				r.dropped.Add(n)
			}
		}
	}
	for e := range r.events {
		flushDropped()
		if !writeLine(e) {
			r.dropped.Add(1)
		}
	}
	flushDropped()
}
