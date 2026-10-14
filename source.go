package liftingcast

// Source is what an in-process consumer needs from a feed of meet states:
// the listeners and the connection status. Both *Hub and *ReplaySource
// satisfy it, so a consumer that accepts a Source works live and against a
// recording.
type Source interface {
	// Listen registers handler as a listener, as Hub.Listen does.
	Listen(handler func(Update)) (stop func())

	// Status returns the current connection status.
	Status() HubStatus
}

var (
	_ Source = (*Hub)(nil)
	_ Source = (*ReplaySource)(nil)
)
