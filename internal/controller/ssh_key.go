package controller

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"

	"golang.org/x/crypto/ssh"
)

type sshKeyMaterial struct {
	PrivateKeyPEM  []byte
	PublicKey      []byte
	AuthorizedKeys []byte
}

func generateSSHKeyMaterial() (sshKeyMaterial, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return sshKeyMaterial{}, fmt.Errorf("generate ssh key: %w", err)
	}
	private := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	if private == nil {
		return sshKeyMaterial{}, fmt.Errorf("encode ssh private key")
	}
	public, err := authorizedKey(&key.PublicKey)
	if err != nil {
		return sshKeyMaterial{}, err
	}
	return sshKeyMaterial{
		PrivateKeyPEM:  private,
		PublicKey:      public,
		AuthorizedKeys: public,
	}, nil
}

func authorizedKey(key *rsa.PublicKey) ([]byte, error) {
	public, err := ssh.NewPublicKey(key)
	if err != nil {
		return nil, fmt.Errorf("encode ssh public key: %w", err)
	}
	out := bytes.TrimSuffix(ssh.MarshalAuthorizedKey(public), []byte("\n"))
	out = append(out, " zfsreplication-controller\n"...)
	return out, nil
}
