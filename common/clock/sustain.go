package clock

import "time"

// Sustain tracks whether a boolean value is consistently true over a dynamic duration. It does this by recording the earliest
// time that it received a true value, and clearing that timestamp any time that a false value is encountered.
// The timestamp is only initialized when a true datapoint is accepted.
type Sustain struct {
	started  time.Time
	source   TimeSource
	duration func() time.Duration
}

func NewSustain(source TimeSource, duration func() time.Duration) Sustain {
	return Sustain{
		source:   source,
		duration: duration,
	}
}

// Check accepts a datapoint and returns true if the condition has been sustained. For example, if the duration was
// 60s, and a true datapoint was accepted every second, it would return false until 60s had elapsed from the first datapoint
// and then subsequently return true.
func (s *Sustain) Check(value bool) bool {
	if value {
		now := s.source.Now()
		if s.started.IsZero() {
			s.started = now
		}
		if now.Sub(s.started) >= s.duration() {
			return true
		}
	} else {
		s.Reset()
	}
	return false
}

// CheckAndReset accepts a datapoint and returns true if the condition has been sustained.
// If the condition has been sustained the timestamp is set to the current time so that it will be considered sustained
// again until after the duration again elapses.
// For example, if the duration was 60s, and a true datapoint was accepted every second, it would return true once every 60s and
// otherwise return false
func (s *Sustain) CheckAndReset(value bool) bool {
	if value {
		now := s.source.Now()
		if s.started.IsZero() {
			s.started = now
		}
		if now.Sub(s.started) >= s.duration() {
			s.started = now
			return true
		}
	} else {
		s.Reset()
	}
	return false
}

// Reset clears the datapoints that the Sustain has received. It is equivalent to providing a false datapoint
func (s *Sustain) Reset() {
	s.started = time.Time{}
}
