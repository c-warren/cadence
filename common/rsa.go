package common

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"strings"
)

type KeyType string

const (
	KeyTypePrivate KeyType = "private key"

	KeyTypePublic KeyType = "public key"
)

func loadRSAKey(path string, keyType KeyType) (interface{}, error) {
	keyString, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("invalid %s path %s", keyType, path)
	}
	block, _ := pem.Decode(keyString)
	if block == nil || strings.ToLower(block.Type) != strings.ToLower(string(keyType)) {
		return nil, fmt.Errorf("failed to parse PEM block containing the %s", keyType)
	}

	switch keyType {
	case KeyTypePrivate:
		key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse DER encoded %s: %s", keyType, err.Error())
		}
		return key, nil
	case KeyTypePublic:
		key, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse DER encoded %s: %s", keyType, err.Error())
		}
		return key, nil
	default:
		return nil, fmt.Errorf("invalid Key Type")
	}
}

func LoadRSAPublicKey(path string) (*rsa.PublicKey, error) {
	key, err := loadRSAKey(path, KeyTypePublic)
	if err != nil {
		return nil, err
	}
	return key.(*rsa.PublicKey), err
}

func LoadRSAPrivateKey(path string) (*rsa.PrivateKey, error) {
	key, err := loadRSAKey(path, KeyTypePrivate)
	if err != nil {
		return nil, err
	}
	return key.(*rsa.PrivateKey), err
}
