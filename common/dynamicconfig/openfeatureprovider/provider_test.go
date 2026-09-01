package openfeatureprovider

import (
	"errors"
	"testing"

	"github.com/open-feature/go-sdk/openfeature"
	"github.com/stretchr/testify/assert"
)

func TestRegisterAndGet(t *testing.T) {
	fakeConstructor := func(Decoder) (openfeature.FeatureProvider, error) {
		return nil, nil
	}

	tests := []struct {
		name    string
		setup   func(providerName string)
		wantErr bool
	}{
		{
			name: "first registration succeeds",
		},
		{
			name: "duplicate registration fails",
			setup: func(providerName string) {
				_ = Register(providerName, fakeConstructor)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			providerName := "test-" + t.Name()
			if tt.setup != nil {
				tt.setup(providerName)
			}

			err := Register(providerName, fakeConstructor)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			constructor, ok := Get(providerName)
			assert.True(t, ok)
			assert.NotNil(t, constructor)
		})
	}
}

func TestGet_Unknown(t *testing.T) {
	_, ok := Get("does-not-exist")
	assert.False(t, ok)
}

type fakeDecoder struct {
	err error
}

func (d fakeDecoder) Decode(out any) error {
	return d.err
}

func TestConstructor_PropagatesDecodeError(t *testing.T) {
	wantErr := errors.New("boom")
	constructor := func(cfg Decoder) (openfeature.FeatureProvider, error) {
		var out struct{}
		return nil, cfg.Decode(&out)
	}

	_, err := constructor(fakeDecoder{err: wantErr})
	assert.ErrorIs(t, err, wantErr)
}
