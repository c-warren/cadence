package authorization

import (
	"crypto/sha256"
	"crypto/sha512"
	"hash"

	"github.com/xdg/scram"
)

// NOTE: the code is copied from https://github.com/IBM/sarama/blob/master/examples/sasl_scram_client/scram_client.go

// SHA256 algorithm
var SHA256 scram.HashGeneratorFcn = func() hash.Hash { return sha256.New() }

// SHA512 algorithm
var SHA512 scram.HashGeneratorFcn = func() hash.Hash { return sha512.New() }

// XDGSCRAMClient is the scram client
type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

// Begin creates new client
func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

// Step takes a string provided from a server (or just an empty string for the
// very first conversation step) and attempts to move the authentication
// conversation forward.  It returns a string to be sent to the server or an
// error if the server message is invalid.  Calling Step after a conversation
// completes is also an error.
func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

// Done stops the client
func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}
