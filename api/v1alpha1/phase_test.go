package v1alpha1

import "testing"

func TestPhaseHelpers(t *testing.T) {
	for _, tt := range []struct {
		phase       Phase
		terminal    bool
		active      bool
		receiveTask ReceiveTaskPhase
	}{
		{phase: "", terminal: false, active: true, receiveTask: ""},
		{phase: PhasePending, terminal: false, active: true, receiveTask: ""},
		{phase: PhaseStartingReceiver, terminal: false, active: true, receiveTask: ""},
		{phase: PhaseReceiverReady, terminal: false, active: true, receiveTask: ""},
		{phase: PhaseRunning, terminal: false, active: true, receiveTask: ""},
		{phase: PhaseSucceeded, terminal: true, active: false, receiveTask: ReceiveTaskPhaseCompleted},
		{phase: PhaseFailed, terminal: true, active: false, receiveTask: ReceiveTaskPhaseFailed},
	} {
		t.Run(string(tt.phase), func(t *testing.T) {
			if got := tt.phase.Terminal(); got != tt.terminal {
				t.Fatalf("Terminal() = %v, want %v", got, tt.terminal)
			}
			if got := tt.phase.Active(); got != tt.active {
				t.Fatalf("Active() = %v, want %v", got, tt.active)
			}
			if got := tt.phase.ReceiveTaskTerminalPhase(); got != tt.receiveTask {
				t.Fatalf("ReceiveTaskTerminalPhase() = %q, want %q", got, tt.receiveTask)
			}
		})
	}
}

func TestReceiveTaskPhaseTerminal(t *testing.T) {
	for _, tt := range []struct {
		phase    ReceiveTaskPhase
		terminal bool
	}{
		{phase: "", terminal: false},
		{phase: ReceiveTaskPhasePending, terminal: false},
		{phase: ReceiveTaskPhaseReady, terminal: false},
		{phase: ReceiveTaskPhaseCompleted, terminal: true},
		{phase: ReceiveTaskPhaseFailed, terminal: true},
	} {
		t.Run(string(tt.phase), func(t *testing.T) {
			if got := tt.phase.Terminal(); got != tt.terminal {
				t.Fatalf("Terminal() = %v, want %v", got, tt.terminal)
			}
		})
	}
}
