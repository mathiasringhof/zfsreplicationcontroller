package receiverauthorization

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/mathias/zfsreplicationcontroller/internal/replication"
	"golang.org/x/crypto/ssh"
)

const maximumLeaseHorizon = 35 * time.Minute

// Candidate is the API-neutral authority requested by one Receive Task.
type Candidate struct {
	TaskUID                    string
	AuthorizedPublicKey        string
	ExpiresAt                  time.Time
	TargetDataset              string
	ReceiverDatasetPrefixes    []string
	ReceiveUnmounted           bool
	ReceiveResumable           bool
	AllowRollback              bool
	AllowDestroy               bool
	AllowMount                 bool
	AllowSyncSnapshotDestroy   bool
	AllowTargetSnapshotDestroy bool
	SyncSnapshotIdentifier     string
	Compression                string
}

// Outcome identifies whether one candidate was accepted without exposing its
// compiled authority.
type Outcome struct {
	TaskUID   string
	Rejection string
}

// compilation is a closed set of private grants and isolated candidate
// outcomes ready for activation as one snapshot.
type compilation struct {
	grants   []grant
	outcomes []Outcome
}

type grant struct {
	TaskUID                    string    `json:"taskUID"`
	AuthorizedPublicKey        string    `json:"authorizedPublicKey"`
	ExpiresAt                  time.Time `json:"expiresAt"`
	TargetDataset              string    `json:"targetDataset"`
	ReceiveUnmounted           bool      `json:"receiveUnmounted"`
	ReceiveResumable           bool      `json:"receiveResumable"`
	AllowRollback              bool      `json:"allowRollback"`
	AllowDestroy               bool      `json:"allowDestroy"`
	AllowMount                 bool      `json:"allowMount"`
	AllowSyncSnapshotDestroy   bool      `json:"allowSyncSnapshotDestroy"`
	AllowTargetSnapshotDestroy bool      `json:"allowTargetSnapshotDestroy"`
	SyncSnapshotIdentifier     string    `json:"syncSnapshotIdentifier"`
	Compression                string    `json:"compression"`
}

func compileCandidates(now time.Time, candidates []Candidate) compilation {
	compiled := compilation{outcomes: make([]Outcome, len(candidates))}
	grants := make([]*grant, len(candidates))
	keyOwners := make(map[string][]int)
	uidOwners := make(map[string][]int)
	for i, candidate := range candidates {
		outcome := Outcome{TaskUID: candidate.TaskUID}
		if candidate.TaskUID != "" {
			uidOwners[candidate.TaskUID] = append(uidOwners[candidate.TaskUID], i)
		}
		publicKey, keyErr := canonicalPublicKey(candidate.AuthorizedPublicKey)
		if keyErr == nil {
			keyOwners[publicKey] = append(keyOwners[publicKey], i)
		}
		if candidate.TaskUID == "" {
			outcome.Rejection = "receive task UID is empty"
			compiled.outcomes[i] = outcome
			continue
		}
		if keyErr != nil {
			outcome.Rejection = keyErr.Error()
			compiled.outcomes[i] = outcome
			continue
		}
		compiledGrant, err := compileCandidate(now, candidate, publicKey)
		if err != nil {
			outcome.Rejection = err.Error()
			compiled.outcomes[i] = outcome
			continue
		}
		compiled.outcomes[i] = outcome
		grants[i] = &compiledGrant
	}
	for _, owners := range uidOwners {
		if len(owners) < 2 {
			continue
		}
		for _, owner := range owners {
			compiled.outcomes[owner].Rejection = "receive task UID is ambiguous within the active candidate set"
			grants[owner] = nil
		}
	}
	for _, owners := range keyOwners {
		if len(owners) < 2 {
			continue
		}
		for _, owner := range owners {
			compiled.outcomes[owner].Rejection = "authorized public key is ambiguous within the active candidate set"
			grants[owner] = nil
		}
	}
	for _, compiledGrant := range grants {
		if compiledGrant != nil {
			compiled.grants = append(compiled.grants, *compiledGrant)
		}
	}
	return compiled
}

func compileCandidate(now time.Time, candidate Candidate, publicKey string) (grant, error) {
	compiledGrant := grant{
		TaskUID:                    candidate.TaskUID,
		AuthorizedPublicKey:        publicKey,
		ExpiresAt:                  candidate.ExpiresAt.UTC(),
		TargetDataset:              candidate.TargetDataset,
		ReceiveUnmounted:           candidate.ReceiveUnmounted,
		ReceiveResumable:           candidate.ReceiveResumable,
		AllowRollback:              candidate.AllowRollback,
		AllowDestroy:               candidate.AllowDestroy,
		AllowMount:                 candidate.AllowMount,
		AllowSyncSnapshotDestroy:   candidate.AllowSyncSnapshotDestroy,
		AllowTargetSnapshotDestroy: candidate.AllowTargetSnapshotDestroy,
		SyncSnapshotIdentifier:     candidate.SyncSnapshotIdentifier,
		Compression:                replication.CompressionDefault(candidate.Compression),
	}
	if err := validateCandidateRules(candidate); err != nil {
		return grant{}, err
	}
	if err := validateGrant(now, compiledGrant); err != nil {
		return grant{}, err
	}
	return compiledGrant, nil
}

func validateCandidateRules(candidate Candidate) error {
	for _, prefix := range candidate.ReceiverDatasetPrefixes {
		if !replication.ValidDatasetName(prefix) {
			return fmt.Errorf("invalid receiver dataset prefix %q", prefix)
		}
	}
	if len(candidate.ReceiverDatasetPrefixes) == 0 {
		return nil
	}
	for _, prefix := range candidate.ReceiverDatasetPrefixes {
		if replication.DatasetOrChild(candidate.TargetDataset, prefix) {
			return nil
		}
	}
	return fmt.Errorf("destination dataset is not allowed on this receiver")
}

func validateGrant(now time.Time, compiledGrant grant) error {
	if compiledGrant.TaskUID == "" {
		return fmt.Errorf("receive task UID is empty")
	}
	if _, err := canonicalPublicKey(compiledGrant.AuthorizedPublicKey); err != nil {
		return err
	}
	if !replication.ValidDatasetName(compiledGrant.TargetDataset) {
		return fmt.Errorf("invalid destination dataset %q", compiledGrant.TargetDataset)
	}
	if compiledGrant.Compression == "" || !replication.CompressionSupported(compiledGrant.Compression) {
		return fmt.Errorf("unsupported compression %q", compiledGrant.Compression)
	}
	if compiledGrant.SyncSnapshotIdentifier != "" && !replication.ValidSyncoidIdentifier(compiledGrant.SyncSnapshotIdentifier) {
		return fmt.Errorf("invalid sync snapshot identifier %q", compiledGrant.SyncSnapshotIdentifier)
	}
	if compiledGrant.AllowSyncSnapshotDestroy && compiledGrant.SyncSnapshotIdentifier == "" {
		return fmt.Errorf("sync snapshot destroy requires a sync snapshot identifier")
	}
	if !compiledGrant.ExpiresAt.After(now) {
		return fmt.Errorf("authorization lease is expired")
	}
	if compiledGrant.ExpiresAt.After(now.Add(maximumLeaseHorizon)) {
		return fmt.Errorf("authorization lease exceeds maximum horizon")
	}
	return nil
}

func canonicalPublicKey(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("authorized public key is empty")
	}
	key, _, options, rest, err := ssh.ParseAuthorizedKey([]byte(raw))
	if err != nil {
		return "", fmt.Errorf("parse authorized public key: %w", err)
	}
	if len(options) > 0 {
		return "", fmt.Errorf("authorized public key must not include authorized_keys options")
	}
	if strings.TrimSpace(string(rest)) != "" {
		return "", fmt.Errorf("authorized public key contains trailing data")
	}
	return strings.TrimSpace(string(ssh.MarshalAuthorizedKey(key))), nil
}

func grantID(taskUID string) string {
	sum := sha256.Sum256([]byte(taskUID))
	return "grant-" + hex.EncodeToString(sum[:])
}

func decodeGrant(data []byte) (grant, error) {
	fields, err := decodeGrantFields(data)
	if err != nil {
		return grant{}, err
	}
	for _, field := range []string{
		"taskUID", "authorizedPublicKey", "expiresAt", "targetDataset",
		"receiveUnmounted", "receiveResumable", "allowRollback", "allowDestroy", "allowMount",
		"allowSyncSnapshotDestroy", "allowTargetSnapshotDestroy", "syncSnapshotIdentifier", "compression",
	} {
		value, ok := fields[field]
		if !ok {
			return grant{}, fmt.Errorf("required field %q is missing", field)
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return grant{}, fmt.Errorf("required field %q must not be null", field)
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var compiledGrant grant
	if err := decoder.Decode(&compiledGrant); err != nil {
		return grant{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return grant{}, fmt.Errorf("unexpected trailing receiver grant data")
		}
		return grant{}, err
	}
	return compiledGrant, nil
}

func decodeGrantFields(data []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	opening, ok := token.(json.Delim)
	if !ok || opening != '{' {
		return nil, fmt.Errorf("receiver grant must be a JSON object")
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		name, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("receiver grant field name is malformed")
		}
		if _, exists := fields[name]; exists {
			return nil, fmt.Errorf("receiver grant field %q is duplicated", name)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		fields[name] = value
	}
	closing, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if closing != json.Delim('}') {
		return nil, fmt.Errorf("receiver grant object is malformed")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("unexpected trailing receiver grant data")
		}
		return nil, err
	}
	return fields, nil
}

func escapeAuthorizedKeysOption(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	return strings.ReplaceAll(value, `"`, `\"`)
}
