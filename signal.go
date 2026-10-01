package liftingcast

// signal sends on ch without blocking, dropping the signal if one is already pending.
func signal(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
