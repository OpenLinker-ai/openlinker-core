package authutil

import (
	"errors"
	"testing"
)

type fakeSQLState string

func (f fakeSQLState) Error() string {
	return string(f)
}

func (f fakeSQLState) SQLState() string {
	return string(f)
}

func TestNormalizeEmail(t *testing.T) {
	if got := NormalizeEmail(" User@Example.COM \n"); got != "user@example.com" {
		t.Fatalf("NormalizeEmail = %q", got)
	}
}

func TestHashPasswordAndCompare(t *testing.T) {
	hash, err := HashPassword("password123")
	if err != nil {
		t.Fatalf("HashPassword returned error: %v", err)
	}
	if err := ComparePasswordHash(hash, "password123"); err != nil {
		t.Fatalf("ComparePasswordHash should accept password: %v", err)
	}
	if err := ComparePasswordHash(hash, "wrong"); err == nil {
		t.Fatalf("ComparePasswordHash should reject wrong password")
	}
}

func TestIsUniqueViolation(t *testing.T) {
	if IsUniqueViolation(nil) || IsUniqueViolation(errors.New("plain")) {
		t.Fatalf("plain errors should not be unique violations")
	}
	if !IsUniqueViolation(fakeSQLState("23505")) || IsUniqueViolation(fakeSQLState("23503")) {
		t.Fatalf("SQLState handling failed")
	}
}
