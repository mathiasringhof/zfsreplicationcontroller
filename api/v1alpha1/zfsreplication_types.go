package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

type Phase string

const (
	PhasePending          Phase = "Pending"
	PhaseStartingReceiver Phase = "StartingReceiver"
	PhaseReceiverReady    Phase = "ReceiverReady"
	PhaseRunning          Phase = "Running"
	PhaseSucceeded        Phase = "Succeeded"
	PhaseFailed           Phase = "Failed"
)

type DatasetRef struct {
	NodeName string `json:"nodeName"`
	Dataset  string `json:"dataset"`
}

type SyncoidSpec struct {
	NoSyncSnap       *bool    `json:"noSyncSnap,omitempty"`
	NoRollback       *bool    `json:"noRollback,omitempty"`
	ForceDelete      *bool    `json:"forceDelete,omitempty"`
	Compress         string   `json:"compress,omitempty"`
	ReceiveUnmounted *bool    `json:"receiveUnmounted,omitempty"`
	ReceiveResumable *bool    `json:"receiveResumable,omitempty"`
	IncludeSnaps     []string `json:"includeSnaps,omitempty"`
	ExcludeSnaps     []string `json:"excludeSnaps,omitempty"`
}

type ZFSReplicationRunSpec struct {
	Source  DatasetRef  `json:"source"`
	Target  DatasetRef  `json:"target"`
	Syncoid SyncoidSpec `json:"syncoid,omitempty"`
}

type ZFSReplicationRunStatus struct {
	Phase           Phase        `json:"phase,omitempty"`
	SenderJobName   string       `json:"senderJobName,omitempty"`
	ReceiverJobName string       `json:"receiverJobName,omitempty"`
	ReceiverPodName string       `json:"receiverPodName,omitempty"`
	ReceiverPodIP   string       `json:"receiverPodIP,omitempty"`
	SSHSecretName   string       `json:"sshSecretName,omitempty"`
	StartedAt       *metav1.Time `json:"startedAt,omitempty"`
	CompletedAt     *metav1.Time `json:"completedAt,omitempty"`
	LastError       string       `json:"lastError,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type ZFSReplicationRun struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ZFSReplicationRunSpec   `json:"spec,omitempty"`
	Status ZFSReplicationRunStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ZFSReplicationRunList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ZFSReplicationRun `json:"items"`
}

type ConcurrencyPolicy string

const (
	ConcurrencyPolicyAllow  ConcurrencyPolicy = "Allow"
	ConcurrencyPolicyForbid ConcurrencyPolicy = "Forbid"
)

type ZFSReplicationScheduleSpec struct {
	Schedule          string                `json:"schedule"`
	Suspend           *bool                 `json:"suspend,omitempty"`
	ConcurrencyPolicy ConcurrencyPolicy     `json:"concurrencyPolicy,omitempty"`
	RunTemplate       ZFSReplicationRunSpec `json:"runTemplate"`
}

type ZFSReplicationScheduleStatus struct {
	LastScheduleTime *metav1.Time `json:"lastScheduleTime,omitempty"`
	LastRunName      string       `json:"lastRunName,omitempty"`
	LastError        string       `json:"lastError,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type ZFSReplicationSchedule struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ZFSReplicationScheduleSpec   `json:"spec,omitempty"`
	Status ZFSReplicationScheduleStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ZFSReplicationScheduleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ZFSReplicationSchedule `json:"items"`
}
