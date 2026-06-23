package credential

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
)

const (
	AccessTokenPrefix = "ol_live_"
	RandomBytes       = 32
	PrefixLen         = len(AccessTokenPrefix) + 4

	LegacyAPIKeyPrefix       = "sk_live_"
	LegacyRegistrationPrefix = "br_live_"
	LegacyAgentPrefix        = "rt_live_"

	// BcryptCost matches user passwords and cloud API keys.
	BcryptCost = 12
)

func GenerateAccessToken() (plaintext, prefix string, err error) {
	raw := make([]byte, RandomBytes)
	if _, err = rand.Read(raw); err != nil {
		return "", "", err
	}
	plaintext = AccessTokenPrefix + hex.EncodeToString(raw)
	return plaintext, plaintext[:PrefixLen], nil
}

func ValidLength(token string) bool {
	return len(strings.TrimSpace(token)) == len(AccessTokenPrefix)+RandomBytes*2
}

func HasAnyPrefix(token string, prefixes ...string) bool {
	token = strings.TrimSpace(token)
	for _, prefix := range prefixes {
		if strings.HasPrefix(token, prefix) {
			return true
		}
	}
	return false
}
