// Package unleash is an openfeatureprovider plugin backed by Unleash
// (https://www.getunleash.io/). Blank-import this package to register it:
//
//	import _ "github.com/uber/cadence/common/dynamicconfig/openfeatureprovider/unleash"
package unleash

import (
	"fmt"

	"github.com/uber/cadence/common/dynamicconfig/openfeatureprovider"
)

func init() {
	if err := openfeatureprovider.Register(ProviderName, newProvider); err != nil {
		panic(fmt.Errorf("failed to register openfeature unleash provider: %w", err))
	}
}
