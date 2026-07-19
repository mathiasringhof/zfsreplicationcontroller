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

	if got := cap(capture.stderr.pending); got > outputTailBytes {
		t.Fatalf("pending capacity = %d, want at most %d", got, outputTailBytes)
	}
}
