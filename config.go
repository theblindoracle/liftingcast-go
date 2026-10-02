package liftingcast

import (
	"log/slog"
	"time"
)

// Config is what one upstream connection needs: where LiftingCast is, which
// meet to follow, and the credentials for that meet.
type Config struct {
	// BaseURL is LiftingCast's WebSocket endpoint, for example
	// "wss://liftingcast.com/websocket".
	BaseURL  string
	MeetID   string
	Password string
	APIKey   string
}

// Option configures a Client or a Hub.
type Option func(*options)

type options struct {
	logger     *slog.Logger
	maxBackoff time.Duration
}

// WithLogger sends the Client's or Hub's logs to logger. Without it they log
// nothing. A Hub passes its logger on to the Clients it starts.
func WithLogger(logger *slog.Logger) Option {
	return func(o *options) {
		if logger != nil {
			o.logger = logger
		}
	}
}

// WithMaxBackoff makes d the longest a Client waits between reconnect
// attempts, instead of the default 5s. A d below the initial 2s wait caps that
// too; a d of zero or less keeps the default. A Hub passes it on to the
// Clients it starts.
//
// On Hosted LiftingCast, two Clients sharing an API key keep pushing each
// other off; a higher cap makes that happen less often.
func WithMaxBackoff(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.maxBackoff = d
		}
	}
}

func newOptions(opts []Option) options {
	o := options{logger: slog.New(slog.DiscardHandler), maxBackoff: maxBackoff}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
