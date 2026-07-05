package credential

import (
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

func TestGenerateUserAndAgentTokenShape(t *testing.T) {
	tests := []struct {
		name     string
		prefix   string
		generate func() (string, string, error)
	}{
		{name: "user", prefix: UserTokenPrefix, generate: GenerateUserToken},
		{name: "agent", prefix: AgentTokenPrefix, generate: GenerateAgentToken},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plaintext, prefix, err := tt.generate()
			if err != nil {
				t.Fatalf("generate returned error: %v", err)
			}
			if !strings.HasPrefix(plaintext, tt.prefix) {
				t.Fatalf("token prefix = %q, want %q", plaintext[:len(tt.prefix)], tt.prefix)
			}
			if len(plaintext) != len(tt.prefix)+RandomBytes*2 {
				t.Fatalf("token length = %d", len(plaintext))
			}
			if prefix != plaintext[:PrefixLen] {
				t.Fatalf("prefix = %q, want plaintext[:PrefixLen]", prefix)
			}
			if !ValidLengthForPrefix(plaintext, tt.prefix) {
				t.Fatalf("generated token should have valid length")
			}
		})
	}
}

func TestValidLengthForPrefixTrimsWhitespace(t *testing.T) {
	token := UserTokenPrefix + strings.Repeat("a", RandomBytes*2)
	if !ValidLengthForPrefix(" \t"+token+"\n", UserTokenPrefix) {
		t.Fatalf("trimmed token should have valid length")
	}
	if ValidLengthForPrefix(token+"a", UserTokenPrefix) {
		t.Fatalf("overlong token should be invalid")
	}
	if ValidLengthForPrefix(token, AgentTokenPrefix) {
		t.Fatalf("user token must not validate as agent token")
	}
}

func TestHasAnyPrefixTrimsAndMatchesKnownPrefixes(t *testing.T) {
	if !HasAnyPrefix("  "+AgentTokenPrefix+"abc", UserTokenPrefix, AgentTokenPrefix) {
		t.Fatalf("expected agent prefix match")
	}
	if HasAnyPrefix("unknown_live_abc", UserTokenPrefix, AgentTokenPrefix) {
		t.Fatalf("unexpected prefix match")
	}
}

func TestBcryptCostMatchesPasswordPolicy(t *testing.T) {
	if BcryptCost != 12 {
		t.Fatalf("BcryptCost = %d, want 12", BcryptCost)
	}
}

func TestFastTokenHashVerifiesTrimmedToken(t *testing.T) {
	token := AgentTokenPrefix + strings.Repeat("a", RandomBytes*2)
	hash := FastTokenHash(" \t" + token + "\n")
	if !strings.HasPrefix(hash, FastTokenHashPrefix) {
		t.Fatalf("hash prefix = %q, want %q", hash[:len(FastTokenHashPrefix)], FastTokenHashPrefix)
	}
	if !VerifyFastTokenHash(hash, token) {
		t.Fatalf("fast hash should verify original token")
	}
	if !VerifyFastTokenHash(hash, "\n"+token+" ") {
		t.Fatalf("fast hash should verify trimmed token")
	}
	if VerifyFastTokenHash(hash, token+"0") {
		t.Fatalf("fast hash should reject wrong token")
	}
	if VerifyFastTokenHash("sha256:short", token) {
		t.Fatalf("fast hash should reject malformed digest")
	}
}

func TestVerifyTokenHashAcceptsFastAndLegacyBcryptHashes(t *testing.T) {
	token := AgentTokenPrefix + strings.Repeat("b", RandomBytes*2)
	if !VerifyTokenHash(FastTokenHash(token), token) {
		t.Fatalf("VerifyTokenHash should accept fast hash")
	}
	legacy, err := bcrypt.GenerateFromPassword(BcryptTokenInput(token), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("GenerateFromPassword returned error: %v", err)
	}
	if !VerifyTokenHash(string(legacy), token) {
		t.Fatalf("VerifyTokenHash should accept legacy bcrypt hash")
	}
	if VerifyTokenHash(FastTokenHash(token), token+"0") {
		t.Fatalf("VerifyTokenHash should reject wrong token")
	}
}
