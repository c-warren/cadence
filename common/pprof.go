package common

//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination pprof_mock.go -package common github.com/uber/cadence/common PProfInitializer

type (
	// PProfInitializer initialize the pprof based on config
	PProfInitializer interface {
		Start() error
	}
)
