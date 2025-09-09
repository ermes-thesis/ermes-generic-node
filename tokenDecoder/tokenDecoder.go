package tokenDecoder

import (
	"crypto/rsa"
	"errors"
	"fmt"
	"github.com/golang-jwt/jwt/v5"
	"os"
)

type GroupTokenClaims struct {
	jwt.RegisteredClaims
	Groups []string `json:"groups"`
}

type TokenDecoder interface {
	DecodeGroupToken(token string) ([]string, error)
	LoadPublicKey(path string) error
}

type TokenDecoderImpl struct {
	publicKey *rsa.PublicKey
	issuer    string
}

func NewTokenDecoder(path string) (TokenDecoder, error) {
	tknDecoder := &TokenDecoderImpl{
		publicKey: nil,
		issuer:    "ermes-central-repository",
	}

	err := tknDecoder.LoadPublicKey(path)
	if err != nil {
		return nil, fmt.Errorf("error loading public key: %s", err)
	}
	return tknDecoder, nil
}

func NewTokenDecoderWithKey(publicKey *rsa.PublicKey, issuer string) TokenDecoder {
	return &TokenDecoderImpl{
		publicKey: publicKey,
		issuer:    issuer,
	}
}

func (t *TokenDecoderImpl) DecodeGroupToken(tokenString string) ([]string, error) {
	var claims *GroupTokenClaims
	token, err := jwt.ParseWithClaims(tokenString, &GroupTokenClaims{}, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return t.publicKey, nil
	})
	switch {
	case errors.Is(err, jwt.ErrTokenMalformed):
		return nil, fmt.Errorf("token is malformed")
	case errors.Is(err, jwt.ErrTokenSignatureInvalid):
		return nil, fmt.Errorf("token signature is invalid")
	case errors.Is(err, jwt.ErrTokenExpired) || errors.Is(err, jwt.ErrTokenNotValidYet):
		return nil, fmt.Errorf("token expired")
	default:
		if err != nil {
			return nil, err
		}
		var ok bool
		claims, ok = token.Claims.(*GroupTokenClaims)
		if !ok || !token.Valid {
			return nil, fmt.Errorf("invalid token")
		}
	}

	issuer, err := claims.GetIssuer()
	if err != nil {
		return nil, err
	}
	if issuer != t.issuer {
		return nil, fmt.Errorf("invalid issuer")
	}

	return claims.Groups, nil
}

func (t *TokenDecoderImpl) LoadPublicKey(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("error reading file: %s", err)
	}

	key, err := jwt.ParseRSAPublicKeyFromPEM(data)

	t.publicKey = key
	return nil
}
