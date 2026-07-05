package credential

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

const (
	UserTokenPrefix  = "ol_user_"
	AgentTokenPrefix = "ol_agent_"
	RandomBytes      = 32
	PrefixLen        = len(UserTokenPrefix) + 4

	// BcryptCost is the default cost for persisted credential hashes.
	BcryptCost = 12

	FastTokenHashPrefix = "sha256:"
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

func FastTokenHash(token string) string {
	return FastTokenHashPrefix + string(BcryptTokenInput(token))
}

func VerifyFastTokenHash(storedHash, token string) bool {
	digest, ok := strings.CutPrefix(strings.TrimSpace(storedHash), FastTokenHashPrefix)
	if !ok || len(digest) != sha256.Size*2 {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(digest), BcryptTokenInput(token)) == 1
}

func VerifyTokenHash(storedHash, token string) bool {
	if VerifyFastTokenHash(storedHash, token) {
		return true
	}
	if strings.HasPrefix(strings.TrimSpace(storedHash), FastTokenHashPrefix) {
		return false
	}
	return bcrypt.CompareHashAndPassword([]byte(storedHash), BcryptTokenInput(token)) == nil
}
