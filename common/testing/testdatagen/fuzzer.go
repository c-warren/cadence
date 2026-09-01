package testdatagen

import (
	"testing"
	"time"

	fuzz "github.com/google/gofuzz"
)

// NewFuzzer creates a new fuzzer, notes down the deterministic seed
func New(t *testing.T, generatorFuncs ...interface{}) *fuzz.Fuzzer {
	return NewWithNilChance(t, time.Now().UnixNano(), 0.2, generatorFuncs...)
}

// NewFuzzer creates a new fuzzer, notes down the deterministic seed
func NewWithNilChance(t *testing.T, seed int64, nilchance float32, generatorFuncs ...interface{}) *fuzz.Fuzzer {
	t.Log("Fuzz Seed:", seed)
	return fuzz.NewWithSeed(seed).Funcs(generatorFuncs...).NilChance(float64(nilchance))
}
