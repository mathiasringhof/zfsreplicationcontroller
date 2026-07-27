package replication

import "testing"

func TestValidDatasetName(t *testing.T) {
	for _, dataset := range []string{
		"tank/app",
		"tank/app/child",
		"tank-1/app_2",
		"tank/app.with:colon",
	} {
		t.Run("valid/"+dataset, func(t *testing.T) {
			if !ValidDatasetName(dataset) {
				t.Fatalf("ValidDatasetName(%q) = false, want true", dataset)
			}
		})
	}

	for _, dataset := range []string{
		"",
		"/tank/app",
		"tank/app/",
		"tank//app",
		"tank/.",
		"tank/..",
		"tank/app@snap",
		"tank/a#b",
		"tank/a*b",
		"tank/a\"b",
		"tank/a[b",
		"tank/a?b",
		"tank/a b",
		"tank/a\x01b",
	} {
		t.Run("invalid/"+dataset, func(t *testing.T) {
			if ValidDatasetName(dataset) {
				t.Fatalf("ValidDatasetName(%q) = true, want false", dataset)
			}
		})
	}
}

func TestDatasetAndSnapshotHelpers(t *testing.T) {
	dataset, snapshot, ok := SplitSnapshotTarget("tank/app@syncoid_rel123_worker_2026")
	if !ok || dataset != "tank/app" || snapshot != "syncoid_rel123_worker_2026" {
		t.Fatalf("SplitSnapshotTarget() = %q, %q, %v", dataset, snapshot, ok)
	}
	for _, value := range []string{"tank/app", "tank/app@snap@again", "tank/app@bad,snap"} {
		if _, _, ok := SplitSnapshotTarget(value); ok {
			t.Fatalf("SplitSnapshotTarget(%q) ok = true, want false", value)
		}
	}
	if !DatasetOrChild("tank/app/child", "tank/app") {
		t.Fatalf("DatasetOrChild(tank/app/child, tank/app) = false, want true")
	}
	if DatasetOrChild("tank/app2", "tank/app") {
		t.Fatalf("DatasetOrChild(tank/app2, tank/app) = true, want false")
	}
	if got := TargetPool("tank/app/child"); got != "tank" {
		t.Fatalf("TargetPool() = %q, want tank", got)
	}
}
