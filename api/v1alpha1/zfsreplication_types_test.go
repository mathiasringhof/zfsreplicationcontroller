package v1alpha1

import "testing"

func TestScheduleDeepCopyCopiesHistoryLimitPointers(t *testing.T) {
	successful := int32(3)
	failed := int32(1)
	schedule := &ZFSReplicationSchedule{
		Spec: ZFSReplicationScheduleSpec{
			SuccessfulRunsHistoryLimit: &successful,
			FailedRunsHistoryLimit:     &failed,
		},
	}

	copy := schedule.DeepCopy()
	if copy.Spec.SuccessfulRunsHistoryLimit == schedule.Spec.SuccessfulRunsHistoryLimit {
		t.Fatalf("SuccessfulRunsHistoryLimit pointer was aliased")
	}
	if copy.Spec.FailedRunsHistoryLimit == schedule.Spec.FailedRunsHistoryLimit {
		t.Fatalf("FailedRunsHistoryLimit pointer was aliased")
	}

	*schedule.Spec.SuccessfulRunsHistoryLimit = 9
	*schedule.Spec.FailedRunsHistoryLimit = 8

	if got := *copy.Spec.SuccessfulRunsHistoryLimit; got != 3 {
		t.Fatalf("copied successful limit = %d, want 3", got)
	}
	if got := *copy.Spec.FailedRunsHistoryLimit; got != 1 {
		t.Fatalf("copied failed limit = %d, want 1", got)
	}
}
