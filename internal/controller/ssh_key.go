package controller

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"math/big"
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
	public := authorizedKey(&key.PublicKey)
	return sshKeyMaterial{
		PrivateKeyPEM:  private,
		PublicKey:      public,
		AuthorizedKeys: public,
	}, nil
}

func authorizedKey(key *rsa.PublicKey) []byte {
	wire := appendSSHString(nil, []byte("ssh-rsa"))
	wire = appendSSHMPInt(wire, big.NewInt(int64(key.E)))
	wire = appendSSHMPInt(wire, key.N)
	encoded := base64.StdEncoding.EncodeToString(wire)
	return []byte("ssh-rsa " + encoded + " zfsreplication-controller\n")
}

func appendSSHString(out, value []byte) []byte {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	out = append(out, length[:]...)
	return append(out, value...)
}

func appendSSHMPInt(out []byte, value *big.Int) []byte {
	bytes := value.Bytes()
	if len(bytes) > 0 && bytes[0]&0x80 != 0 {
		bytes = append([]byte{0}, bytes...)
	}
	return appendSSHString(out, bytes)
}
