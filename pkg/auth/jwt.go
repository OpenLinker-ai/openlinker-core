package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const jwtIssuer = "openlinker"

// Claims JWT payload。
//
//	sub = user_id (UUID 字符串)
//	iat / exp 由 RegisteredClaims 提供
type Claims struct {
	jwt.RegisteredClaims
	TokenVersion int64 `json:"token_version"`
}

// GenerateToken 用 HS256 签发 JWT。
func GenerateToken(userID, secret string, ttl time.Duration) (string, error) {
	return GenerateTokenWithVersion(userID, secret, ttl, 0)
}

// GenerateTokenWithVersion signs a user-session JWT bound to the current
// durable users.token_version value.
func GenerateTokenWithVersion(userID, secret string, ttl time.Duration, tokenVersion int64) (string, error) {
	if tokenVersion < 0 {
		return "", errors.New("jwt token version must be non-negative")
	}
	now := time.Now()
	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    jwtIssuer,
			Subject:   userID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		},
		TokenVersion: tokenVersion,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(secret))
	if err != nil {
		return "", fmt.Errorf("sign jwt: %w", err)
	}
	return signed, nil
}

// ParseTokenClaims verifies a user-session JWT and returns its complete claims.
func ParseTokenClaims(tokenStr, secret string) (*Claims, error) {
	parsed, err := jwt.ParseWithClaims(tokenStr, &Claims{}, func(t *jwt.Token) (interface{}, error) {
		if t.Method != jwt.SigningMethodHS256 {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return []byte(secret), nil
	}, jwt.WithIssuer(jwtIssuer), jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}))
	if err != nil {
		return nil, err
	}
	claims, ok := parsed.Claims.(*Claims)
	if !ok || !parsed.Valid {
		return nil, errors.New("invalid jwt")
	}
	if claims.Subject == "" {
		return nil, errors.New("jwt missing sub")
	}
	if claims.TokenVersion < 0 {
		return nil, errors.New("jwt token version must be non-negative")
	}
	return claims, nil

}

// ParseToken 校验签名 + 过期，返回 sub (user_id)。
func ParseToken(tokenStr, secret string) (string, error) {
	claims, err := ParseTokenClaims(tokenStr, secret)
	if err != nil {
		return "", err
	}
	return claims.Subject, nil
}
