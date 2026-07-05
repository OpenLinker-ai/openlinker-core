package authutil

import (
	"errors"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

const BcryptCost = 12

func NormalizeEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

func HashPassword(password string) (string, error) {
	hashed, err := bcrypt.GenerateFromPassword([]byte(password), BcryptCost)
	if err != nil {
		return "", err
	}
	return string(hashed), nil
}

func ComparePasswordHash(hash, password string) error {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
}

func IsUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	type sqlState interface{ SQLState() string }
	var ss sqlState
	return errors.As(err, &ss) && ss.SQLState() == "23505"
}
