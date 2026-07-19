package diagnosis

import (
	"bytes"
	"testing"
)

func TestCaptureDoesNotRetainSingleOversizedWrite(t *testing.T) {
	capture := NewCapture(nil)

	if _, err := capture.Stderr().Write(bytes.Repeat([]byte{'x'}, outputTailBytes*4)); err != nil {
		t.Fatal(err)
	}

	// Retained capacity is intentionally inspected: bounded allocation is part of
	// Capture's contract, but its violation is not observable in emitted output.
	if got := cap(capture.stderr.pending); got > outputTailBytes {
		t.Fatalf("pending capacity = %d, want at most %d", got, outputTailBytes)
	}
}
