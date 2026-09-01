package postgres

import "testing"

var testCases = []struct {
	name     string
	username string
	password string
	want     string
}{
	{
		name:     "default",
		username: "cadence",
		password: "cadence",
		want:     "cadence:cadence",
	},
	{
		name:     "with forward slash",
		username: "cadence",
		password: "cad/ence",
		want:     "cadence:cad%2Fence",
	},
	{
		name:     "with question mark",
		username: "cadence",
		password: "cad?ence",
		want:     "cadence:cad%3Fence",
	},
}

func TestGenerateCredentialString(t *testing.T) {
	for _, tc := range testCases {
		if userPass := generateCredentialString(tc.username, tc.password); userPass != tc.want {
			t.Errorf("%v: got %v, want %v", tc.name, userPass, tc.want)
		}
	}
}
