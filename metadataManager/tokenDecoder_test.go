package metadataManager_test

import (
	"crypto/rand"
	"crypto/rsa"
	"metadataManager"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
)

func TestDecodeGroupToken_Success(t *testing.T) {
	// Generate test RSA keys
	privKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	pubKey := &privKey.PublicKey

	// Create a JWT
	claims := metadataManager.GroupTokenClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "ermes-central-repository",
			Subject:   "userID",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			ID:        "tokenID",
		},
		Groups: []string{"test", "try"},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tokenStr, err := token.SignedString(privKey)
	require.NoError(t, err)

	// Build decoder with public key
	decoder, err := metadataManager.NewTokenDecoderWithKey(pubKey, "ermes-central-repository")
	require.NoError(t, err)

	// Decode
	groups, err := decoder.DecodeGroupToken(tokenStr)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"test", "try"}, groups)
}

func TestDecodeGroupToken_Expired(t *testing.T) {
	privKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	pubKey := &privKey.PublicKey

	claims := metadataManager.GroupTokenClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "ermes-central-repository",
			Subject:   "userID",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Hour)),
			ID:        "tokenID",
		},
		Groups: []string{"test", "try"},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tokenStr, err := token.SignedString(privKey)
	require.NoError(t, err)

	// Build decoder with public key
	decoder, err := metadataManager.NewTokenDecoderWithKey(pubKey, "ermes-central-repository")
	require.NoError(t, err)

	// Decode
	_, err = decoder.DecodeGroupToken(tokenStr)
	require.EqualError(t, err, "token expired")
}
