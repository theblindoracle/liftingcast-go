package liftingcast

import "log/slog"

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
	logger *slog.Logger
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

func newOptions(opts []Option) options {
	o := options{logger: slog.New(slog.DiscardHandler)}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
