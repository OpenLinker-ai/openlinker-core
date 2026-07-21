package runtimepki

import (
	"strings"
	"testing"
)

func TestValidateCredentialRequestRequiresClientPersistedNodeID(t *testing.T) {
	_, _, err := validateCredentialRequest(&CredentialRequest{})
	if err == nil || !strings.Contains(err.Error(), "node_id 必填") {
		t.Fatalf("empty node_id error = %v", err)
	}
}
