// Command lc-tap records the raw traffic of an upstream connection to
// LiftingCast, including everything the Client hides: the handshake status,
// messages that aren't meet state, and how and when each connection closed.
// It dials LiftingCast directly rather than through the Client for exactly
// that reason.
//
// It writes one JSON object per line. Meet state keeps the shape of the
// existing recordings in testdata/, so they replay the same way:
//
//	{"rx":"…","conn":1,"bytes":76091,"msg":{…}}
//
// Everything else is an event:
//
//	{"rx":"…","conn":1,"event":"handshake","status":101}
//	{"rx":"…","conn":1,"event":"handshake","status":401,"error":"…","body":"…"}
//	{"rx":"…","conn":1,"event":"text","bytes":23,"text":"…"}   message that isn't JSON
//	{"rx":"…","conn":1,"event":"close","code":1008,"text":"…"} close frame from the server
//	{"rx":"…","conn":1,"event":"read_error","error":"…"}       dropped without a close frame
//	{"rx":"…","conn":1,"event":"stop"}                         stopped with Ctrl-C
//
// After a connection ends or a dial fails it waits and dials again, up to
// -reconnects times, so a recording shows what happens on reconnect. Pongs are
// not recorded.
//
// Credentials come from the environment and are never written to the
// recording: LC_WS_URL, LC_MEET_ID, LC_PASSWORD, LC_API_KEY.
//
// Usage:
//
//	go run ./cmd/lc-tap [-reconnects 2] [-wait 2s] out.jsonl
//
// For two connections at once, run it twice with the same credentials and
// different output files.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Same timings as the Client
const (
	pingInterval     = 30 * time.Second
	handshakeTimeout = 10 * time.Second
	writeWait        = 10 * time.Second

	// Most of a refused handshake's body that is recorded
	maxBody = 4096
)

type config struct {
	wsURL, meetID, password, apiKey string
	reconnects                      int
	wait                            time.Duration
}

func main() {
	reconnects := flag.Int("reconnects", 2, "times to dial again after a connection ends or a dial fails")
	wait := flag.Duration("wait", 2*time.Second, "wait before each reconnect")
	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), "usage: lc-tap [flags] out.jsonl")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}

	cfg := config{
		wsURL:      os.Getenv("LC_WS_URL"),
		meetID:     os.Getenv("LC_MEET_ID"),
		password:   os.Getenv("LC_PASSWORD"),
		apiKey:     os.Getenv("LC_API_KEY"),
		reconnects: *reconnects,
		wait:       *wait,
	}
	if cfg.wsURL == "" || cfg.meetID == "" {
		log.Fatal("LC_WS_URL and LC_MEET_ID must be set")
	}

	f, err := os.Create(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := tap(ctx, cfg, f); err != nil {
		log.Fatal(err)
	}
}

// entry is one line of a recording.
type entry struct {
	Rx     string          `json:"rx"`
	Conn   int             `json:"conn"`
	Event  string          `json:"event,omitempty"`
	Status int             `json:"status,omitempty"`
	Code   int             `json:"code,omitempty"`
	Bytes  int             `json:"bytes,omitempty"`
	Msg    json.RawMessage `json:"msg,omitempty"`
	Text   string          `json:"text,omitempty"`
	Error  string          `json:"error,omitempty"`
	Body   string          `json:"body,omitempty"`
}

// recorder writes one line per entry, with every credential scrubbed from
// the text it records.
type recorder struct {
	mu     sync.Mutex
	w      io.Writer
	secret []string
}

func (r *recorder) write(e entry) error {
	e.Rx = time.Now().UTC().Format(time.RFC3339Nano)
	e.Text, e.Error, e.Body = r.redact(e.Text), r.redact(e.Error), r.redact(e.Body)
	line, err := json.Marshal(e)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	_, err = fmt.Fprintf(r.w, "%s\n", line)
	return err
}

// redact skips secrets under 4 characters, which would match too much
// unrelated text.
func (r *recorder) redact(s string) string {
	for _, secret := range r.secret {
		if len(secret) >= 4 {
			s = strings.ReplaceAll(s, secret, "REDACTED")
		}
	}
	return s
}

// tap records connections until it has dialled 1+reconnects times or ctx is
// cancelled.
func tap(ctx context.Context, cfg config, w io.Writer) error {
	u, err := url.Parse(cfg.wsURL)
	if err != nil {
		return err
	}
	auth := base64.StdEncoding.EncodeToString([]byte(cfg.meetID + ":" + cfg.password))
	q := u.Query()
	q.Set("meetId", cfg.meetID)
	q.Set("auth", auth)
	q.Set("apiKey", cfg.apiKey)
	u.RawQuery = q.Encode()

	rec := &recorder{w: w, secret: []string{
		cfg.password, cfg.apiKey, auth, url.QueryEscape(auth), url.QueryEscape(cfg.password), url.QueryEscape(cfg.apiKey),
	}}

	for conn := 1; conn <= 1+cfg.reconnects; conn++ {
		if conn > 1 {
			log.Printf("dialling again in %v", cfg.wait)
			select {
			case <-time.After(cfg.wait):
			case <-ctx.Done():
				return nil
			}
		}
		if err := tapOne(ctx, u.String(), conn, rec); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
	}
	return nil
}

// tapOne dials once and records that connection until it ends. It returns an
// error only if the recording can't be written.
func tapOne(ctx context.Context, wsURL string, n int, rec *recorder) error {
	dialer := websocket.Dialer{ReadBufferSize: 1 << 17, HandshakeTimeout: handshakeTimeout}
	conn, resp, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		if ctx.Err() != nil {
			return rec.write(entry{Conn: n, Event: "stop"})
		}
		e := entry{Conn: n, Event: "handshake", Error: err.Error()}
		if resp != nil {
			e.Status = resp.StatusCode
			body, _ := io.ReadAll(io.LimitReader(resp.Body, maxBody))
			resp.Body.Close()
			e.Body = string(body)
		}
		log.Printf("conn %d: dial failed: %s", n, rec.redact(err.Error()))
		return rec.write(e)
	}
	log.Printf("conn %d: connected", n)
	if err := rec.write(entry{Conn: n, Event: "handshake", Status: resp.StatusCode}); err != nil {
		conn.Close()
		return err
	}

	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(pingInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				conn.SetWriteDeadline(time.Now().Add(writeWait))
				conn.WriteMessage(websocket.TextMessage, []byte("ping"))
			case <-ctx.Done():
				conn.WriteControl(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(writeWait))
				conn.Close()
				return
			case <-done:
				return
			}
		}
	}()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			conn.Close()
			if ctx.Err() != nil {
				log.Printf("conn %d: stopped", n)
				return rec.write(entry{Conn: n, Event: "stop"})
			}
			// gorilla reports a connection that ends without a close frame
			// as code 1006, which no server sends
			var ce *websocket.CloseError
			if errors.As(err, &ce) && ce.Code != websocket.CloseAbnormalClosure {
				log.Printf("conn %d: closed by server: %d %q", n, ce.Code, ce.Text)
				return rec.write(entry{Conn: n, Event: "close", Code: ce.Code, Text: ce.Text})
			}
			log.Printf("conn %d: dropped: %v", n, err)
			return rec.write(entry{Conn: n, Event: "read_error", Error: err.Error()})
		}
		if string(msg) == "pong" {
			continue
		}

		var werr error
		if json.Valid(msg) {
			log.Printf("conn %d: meet state, %d bytes", n, len(msg))
			werr = rec.write(entry{Conn: n, Bytes: len(msg), Msg: msg})
		} else {
			log.Printf("conn %d: text: %.200s", n, rec.redact(string(msg)))
			werr = rec.write(entry{Conn: n, Event: "text", Bytes: len(msg), Text: string(msg)})
		}
		if werr != nil {
			return werr
		}
	}
}
