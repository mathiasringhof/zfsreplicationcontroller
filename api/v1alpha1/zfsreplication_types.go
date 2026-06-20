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

const (
	BootstrapFailIfNoBase                = "FailIfNoBase"
	BootstrapDestroyTargetAndReceiveFull = "DestroyTargetAndReceiveFull"
)

type DatasetRef struct {
	NodeName string `json:"nodeName"`
	Dataset  string `json:"dataset"`
}

type BootstrapSpec struct {
	Mode string `json:"mode,omitempty"`
}

type ReceiveSpec struct {
	ReceiveUnmounted *bool `json:"receiveUnmounted,omitempty"`
	Resumable        *bool `json:"resumable,omitempty"`
}

type ZFSReplicationSpec struct {
	RunID          string        `json:"runID,omitempty"`
	Source         DatasetRef    `json:"source"`
	Target         DatasetRef    `json:"target"`
	SnapshotPrefix string        `json:"snapshotPrefix,omitempty"`
	Bootstrap      BootstrapSpec `json:"bootstrap,omitempty"`
	Receive        ReceiveSpec   `json:"receive,omitempty"`
}

type ZFSReplicationStatus struct {
	Phase                      Phase        `json:"phase,omitempty"`
	ObservedRunID              string       `json:"observedRunID,omitempty"`
	LastAttemptedRunID         string       `json:"lastAttemptedRunID,omitempty"`
	LastSuccessfulRunID        string       `json:"lastSuccessfulRunID,omitempty"`
	LastSuccessfulSnapshot     string       `json:"lastSuccessfulSnapshot,omitempty"`
	LastSuccessfulSnapshotGUID string       `json:"lastSuccessfulSnapshotGUID,omitempty"`
	SenderJobName              string       `json:"senderJobName,omitempty"`
	ReceiverJobName            string       `json:"receiverJobName,omitempty"`
	ReceiverPodName            string       `json:"receiverPodName,omitempty"`
	ReceiverPodIP              string       `json:"receiverPodIP,omitempty"`
	SSHSecretName              string       `json:"sshSecretName,omitempty"`
	StartedAt                  *metav1.Time `json:"startedAt,omitempty"`
	CompletedAt                *metav1.Time `json:"completedAt,omitempty"`
	LastError                  string       `json:"lastError,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type ZFSReplication struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ZFSReplicationSpec   `json:"spec,omitempty"`
	Status ZFSReplicationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type ZFSReplicationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ZFSReplication `json:"items"`
}
