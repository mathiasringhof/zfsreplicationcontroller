package controller

import (
	"bytes"
	"testing"

	"golang.org/x/crypto/ssh"
)

func TestGenerateSSHKeyMaterialProducesParseableOpenSSHKeys(t *testing.T) {
	key, err := generateSSHKeyMaterial()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ssh.ParseRawPrivateKey(key.PrivateKeyPEM); err != nil {
		t.Fatalf("parse private key: %v", err)
	}

	public, comment, options, rest, err := ssh.ParseAuthorizedKey(key.PublicKey)
	if err != nil {
		t.Fatalf("parse public key: %v", err)
	}
	if public.Type() != "ssh-rsa" {
		t.Fatalf("public key type = %q, want ssh-rsa", public.Type())
	}
	if comment != "zfsreplication-controller" {
		t.Fatalf("public key comment = %q, want zfsreplication-controller", comment)
	}
	if len(options) != 0 {
		t.Fatalf("public key options = %v, want none", options)
	}
	if len(rest) != 0 {
		t.Fatalf("public key rest = %q, want empty", rest)
	}
	if !bytes.Equal(key.AuthorizedKeys, key.PublicKey) {
		t.Fatalf("authorized_keys differs from public key")
	}
}
