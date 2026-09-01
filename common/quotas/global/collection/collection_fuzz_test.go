package collection

import (
	"math"
	"testing"

	"golang.org/x/time/rate"
)

func FuzzBoostRPS(f *testing.F) {
	f.Fuzz(func(t *testing.T, target, fallback, weight, used float64) {
		target = math.Abs(target)
		fallback = math.Abs(fallback)
		used = math.Abs(used)
		weight = weight - math.Floor(weight) // trim to 0..1

		if anyInvalid(target, fallback, used, weight) {
			t.Skip("bad numbers")
		}

		if target < fallback {
			// fallback is always equal or below target, as it's `target / num hosts`.
			target, fallback = fallback, target
		}

		boosted := boostRPS(rate.Limit(target), rate.Limit(fallback), weight, used)

		if boosted > rate.Limit(target) {
			// should never exceed whole-cluster target
			t.Error("boosted beyond configured limit")
		}
		if boosted < 0 {
			// should never become negative.
			//
			// the ratelimiter treats negatives as zero, so this is "fine",
			// but it's likely a sign of flawed logic.
			t.Error("boosted is negative")
		}
		if math.IsNaN(float64(boosted)) {
			t.Error("boosted is NaN")
		}
		if math.IsInf(float64(boosted), 0) {
			t.Error("boosted is inf")
		}
	})
}

func anyInvalid(f ...float64) bool {
	for _, v := range f {
		if math.IsNaN(v) {
			return true
		}
		if math.IsInf(v, 0) {
			return true
		}
	}
	return false
}
