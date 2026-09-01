package idlfuzzedtestdata

import (
	"testing"

	fuzz "github.com/google/gofuzz"

	"github.com/uber/cadence/common/testing/testdatagen"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/common/types/testdata"
)

// NewFuzzerWithIDLTypes creates a new fuzzer, notes down the deterministic seed
// this particular invocation is preconfigured to be able to handle idl structs
// correctly without generating completely invalid data (which, while good to test for
// in the context of an application is too wide a search to be useful)
func NewFuzzerWithIDLTypes(t *testing.T) *fuzz.Fuzzer {
	return testdatagen.New(t,
		// USE THESE VERY SPARINGLY, ONLY WHEN YOU MUST!
		//
		// The goal of providing these generators for specific types should be
		// to use them as little as possible, as they are fixed test data
		// which will not evolve with the idl or functions, therefore
		// the main benefit of fuzzing - evolving tests to handle all new fields in place -
		// will be defeated.
		//
		// for example, for mappers, if you add a new field that needs to be
		// mapped from protobuf to a native-go type (from the types folder)
		// and the testdata is fixed here *and not updated*, then the issue
		// will not be caught by any roundtrip tests.
		GenHistoryEvent,
	)
}

// GenHistoryEvent is a function to use with gofuzz which
// skips the majority of difficult to generate values
// for the sake of simplicity in testing. Use it with the fuzz.Funcs(...) generation function
func GenHistoryEvent(o *types.HistoryEvent, c fuzz.Continue) {
	// todo (david.porter) setup an assertion to ensure this list is exhaustive
	i := c.Rand.Intn(len(testdata.HistoryEventArray) - 1)
	o = testdata.HistoryEventArray[i]
	return
}
