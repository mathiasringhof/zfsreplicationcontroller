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

type ReceiveTaskPhase string

const (
	ReceiveTaskPhasePending   ReceiveTaskPhase = "Pending"
	ReceiveTaskPhaseReady     ReceiveTaskPhase = "Ready"
	ReceiveTaskPhaseCompleted ReceiveTaskPhase = "Completed"
	ReceiveTaskPhaseFailed    ReceiveTaskPhase = "Failed"
)

type DatasetRef struct {
	NodeName string `json:"nodeName"`
	Dataset  string `json:"dataset"`
}

type LocalObjectReference struct {
	Name string `json:"name"`
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
	ReceiveTaskName string       `json:"receiveTaskName,omitempty"`
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

type ReceiveDestination struct {
	Dataset string `json:"dataset"`
}

type ReceiveTaskSSHSpec struct {
	AuthorizedPublicKey string      `json:"authorizedPublicKey"`
	ExpiresAt           metav1.Time `json:"expiresAt"`
}

type ReceiveTaskPolicy struct {
	ReceiveUnmounted         bool   `json:"receiveUnmounted"`
	ReceiveResumable         bool   `json:"receiveResumable,omitempty"`
	AllowRollback            bool   `json:"allowRollback,omitempty"`
	AllowDestroy             bool   `json:"allowDestroy,omitempty"`
	AllowMount               bool   `json:"allowMount,omitempty"`
	AllowSyncSnapshotDestroy bool   `json:"allowSyncSnapshotDestroy,omitempty"`
	SyncSnapshotIdentifier   string `json:"syncSnapshotIdentifier,omitempty"`
	Compression              string `json:"compression,omitempty"`
}

type ZFSReceiveTaskSpec struct {
	RunRef      LocalObjectReference `json:"runRef"`
	NodeName    string               `json:"nodeName"`
	Destination ReceiveDestination   `json:"destination"`
	SSH         ReceiveTaskSSHSpec   `json:"ssh"`
	Policy      ReceiveTaskPolicy    `json:"policy,omitempty"`
}

type ReceiveTaskEndpoint struct {
	Host string `json:"host,omitempty"`
	Port int32  `json:"port,omitempty"`
}

type ReceiveTaskSSHStatus struct {
	HostKey string `json:"hostKey,omitempty"`
}

type ReceiveTaskPodStatus struct {
	Name string `json:"name,omitempty"`
	UID  string `json:"uid,omitempty"`
}

type ZFSReceiveTaskStatus struct {
	Phase       ReceiveTaskPhase     `json:"phase,omitempty"`
	Endpoint    ReceiveTaskEndpoint  `json:"endpoint,omitempty"`
	SSH         ReceiveTaskSSHStatus `json:"ssh,omitempty"`
	ReceiverPod ReceiveTaskPodStatus `json:"receiverPod,omitempty"`
	Error       string               `json:"error,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type ZFSReceiveTask struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ZFSReceiveTaskSpec   `json:"spec,omitempty"`
	Status ZFSReceiveTaskStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ZFSReceiveTaskList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ZFSReceiveTask `json:"items"`
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
