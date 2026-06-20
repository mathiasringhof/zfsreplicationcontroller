package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
)

const (
	maxRunIDLength          = 40
	maxSnapshotPrefixLength = 32
)

var runIdentityValue = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

type effectiveReplicationSpec struct {
	RunID            string `json:"runID"`
	SourceNode       string `json:"sourceNode"`
	SourceDataset    string `json:"sourceDataset"`
	TargetNode       string `json:"targetNode"`
	TargetDataset    string `json:"targetDataset"`
	SnapshotPrefix   string `json:"snapshotPrefix"`
	BootstrapMode    string `json:"bootstrapMode"`
	ReceiveUnmounted bool   `json:"receiveUnmounted"`
	ReceiveResumable bool   `json:"receiveResumable"`
}

func validateReplicationSpec(spec zfsv1.ZFSReplicationSpec) error {
	if err := validateRunIdentityValue("spec.runID", spec.RunID, maxRunIDLength, false); err != nil {
		return err
	}
	if err := validateRunIdentityValue("spec.snapshotPrefix", spec.SnapshotPrefix, maxSnapshotPrefixLength, true); err != nil {
		return err
	}
	return nil
}

func validateRunIdentityValue(field, value string, maxLength int, optional bool) error {
	if value == "" {
		if optional {
			return nil
		}
		return fmt.Errorf("%s must not be empty", field)
	}
	if len(value) > maxLength {
		return fmt.Errorf("%s must be at most %d characters", field, maxLength)
	}
	if !runIdentityValue.MatchString(value) {
		return fmt.Errorf("%s must contain only lowercase letters, numbers, and dashes, and must start and end with a letter or number", field)
	}
	return nil
}

func replicationSpecHash(spec zfsv1.ZFSReplicationSpec) string {
	data, err := json.Marshal(effectiveSpec(spec))
	if err != nil {
		panic(fmt.Sprintf("marshal replication spec hash: %v", err))
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func effectiveSpec(spec zfsv1.ZFSReplicationSpec) effectiveReplicationSpec {
	return effectiveReplicationSpec{
		RunID:            spec.RunID,
		SourceNode:       spec.Source.NodeName,
		SourceDataset:    spec.Source.Dataset,
		TargetNode:       spec.Target.NodeName,
		TargetDataset:    spec.Target.Dataset,
		SnapshotPrefix:   snapshotPrefix(spec.SnapshotPrefix),
		BootstrapMode:    bootstrapMode(spec.Bootstrap.Mode),
		ReceiveUnmounted: boolDefault(spec.Receive.ReceiveUnmounted, true),
		ReceiveResumable: boolDefault(spec.Receive.Resumable, true),
	}
}
