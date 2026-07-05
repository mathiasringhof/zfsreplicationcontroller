package controller

import "testing"

func TestFailedPodLogOptionsAreBounded(t *testing.T) {
	opts := failedPodLogOptions()
	if opts.TailLines == nil || *opts.TailLines <= 0 {
		t.Fatalf("TailLines = %v, want positive bound", opts.TailLines)
	}
	if opts.LimitBytes == nil || *opts.LimitBytes <= 0 {
		t.Fatalf("LimitBytes = %v, want positive bound", opts.LimitBytes)
	}
}
