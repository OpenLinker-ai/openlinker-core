package credential

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

const (
	UserTokenPrefix  = "ol_user_"
	AgentTokenPrefix = "ol_agent_"
	RandomBytes      = 32
	PrefixLen        = len(UserTokenPrefix) + 4

	// BcryptCost matches user passwords and cloud User Tokens.
	BcryptCost = 12
)

func GenerateUserToken() (plaintext, prefix string, err error) {
	return generateToken(UserTokenPrefix)
}

func GenerateAgentToken() (plaintext, prefix string, err error) {
	return generateToken(AgentTokenPrefix)
}

func generateToken(tokenPrefix string) (plaintext, prefix string, err error) {
	raw := make([]byte, RandomBytes)
	if _, err = rand.Read(raw); err != nil {
		return "", "", err
	}
	plaintext = tokenPrefix + hex.EncodeToString(raw)
	return plaintext, plaintext[:PrefixLen], nil
}

func ValidLengthForPrefix(token, tokenPrefix string) bool {
	return len(strings.TrimSpace(token)) == len(tokenPrefix)+RandomBytes*2
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

func BcryptTokenInput(token string) []byte {
	sum := sha256.Sum256([]byte(strings.TrimSpace(token)))
	encoded := make([]byte, hex.EncodedLen(len(sum)))
	hex.Encode(encoded, sum[:])
	return encoded
}
