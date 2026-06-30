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
}

// GenerateToken 用 HS256 签发 JWT。
func GenerateToken(userID, secret string, ttl time.Duration) (string, error) {
	now := time.Now()
	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    jwtIssuer,
			Subject:   userID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString([]byte(secret))
	if err != nil {
		return "", fmt.Errorf("sign jwt: %w", err)
	}
	return signed, nil
}

// ParseToken 校验签名 + 过期，返回 sub (user_id)。
func ParseToken(tokenStr, secret string) (string, error) {
	parsed, err := jwt.ParseWithClaims(tokenStr, &Claims{}, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return []byte(secret), nil
	}, jwt.WithIssuer(jwtIssuer))
	if err != nil {
		return "", err
	}
	claims, ok := parsed.Claims.(*Claims)
	if !ok || !parsed.Valid {
		return "", errors.New("invalid jwt")
	}
	if claims.Subject == "" {
		return "", errors.New("jwt missing sub")
	}
	return claims.Subject, nil
}
