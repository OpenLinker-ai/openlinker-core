package credential

import (
	"strings"
	"testing"
)

func TestGenerateAccessTokenShape(t *testing.T) {
	plaintext, prefix, err := GenerateAccessToken()
	if err != nil {
		t.Fatalf("GenerateAccessToken returned error: %v", err)
	}
	if !strings.HasPrefix(plaintext, AccessTokenPrefix) {
		t.Fatalf("token prefix = %q, want %q", plaintext[:len(AccessTokenPrefix)], AccessTokenPrefix)
	}
	if len(plaintext) != len(AccessTokenPrefix)+RandomBytes*2 {
		t.Fatalf("token length = %d", len(plaintext))
	}
	if prefix != plaintext[:PrefixLen] {
		t.Fatalf("prefix = %q, want plaintext[:PrefixLen]", prefix)
	}
	if !ValidLength(plaintext) {
		t.Fatalf("generated token should have valid length")
	}
}

func TestValidLengthTrimsWhitespace(t *testing.T) {
	token := AccessTokenPrefix + strings.Repeat("a", RandomBytes*2)
	if !ValidLength(" \t" + token + "\n") {
		t.Fatalf("trimmed token should have valid length")
	}
	if ValidLength(token + "a") {
		t.Fatalf("overlong token should be invalid")
	}
}

func TestHasAnyPrefixTrimsAndMatchesKnownPrefixes(t *testing.T) {
	if !HasAnyPrefix("  "+LegacyAgentPrefix+"abc", LegacyAPIKeyPrefix, LegacyAgentPrefix) {
		t.Fatalf("expected legacy agent prefix match")
	}
	if HasAnyPrefix("unknown_live_abc", LegacyAPIKeyPrefix, LegacyAgentPrefix) {
		t.Fatalf("unexpected prefix match")
	}
}
