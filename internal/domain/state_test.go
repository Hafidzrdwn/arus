package domain_test

import (
	"arus/internal/domain"
	"errors"
	"testing"
)

func TestStateTransitions_Valid(t *testing.T) {
	sampleErr := domain.NewDownloadError(domain.ErrCodeNetwork, "connection dropped", true)

	tests := []struct {
		name          string
		initialState  domain.State
		event         domain.Event
		expectedState domain.State
		checkError    func(t *testing.T, d *domain.Download)
	}{
		// 1. Queued transitions
		{
			name:          "Queued -> Probing on SlotGranted",
			initialState:  domain.StateQueued,
			event:         domain.SlotGranted{},
			expectedState: domain.StateProbing,
		},
		{
			name:          "Queued -> Cancelled on Cancel",
			initialState:  domain.StateQueued,
			event:         domain.Cancel{},
			expectedState: domain.StateCancelled,
		},
		{
			name:          "Queued -> Failed on Fatal",
			initialState:  domain.StateQueued,
			event:         domain.Fatal{Err: sampleErr},
			expectedState: domain.StateFailed,
			checkError: func(t *testing.T, d *domain.Download) {
				if d.Err != sampleErr {
					t.Errorf("expected Err to be %v, got %v", sampleErr, d.Err)
				}
			},
		},

		// 2. Probing transitions
		{
			name:          "Probing -> Allocating on ProbeOK(new)",
			initialState:  domain.StateProbing,
			event:         domain.ProbeOK{Resume: false},
			expectedState: domain.StateAllocating,
		},
		{
			name:          "Probing -> Downloading on ProbeOK(resume)",
			initialState:  domain.StateProbing,
			event:         domain.ProbeOK{Resume: true},
			expectedState: domain.StateDownloading,
		},
		{
			name:          "Probing -> Failed on Fatal",
			initialState:  domain.StateProbing,
			event:         domain.Fatal{Err: sampleErr},
			expectedState: domain.StateFailed,
			checkError: func(t *testing.T, d *domain.Download) {
				if d.Err != sampleErr {
					t.Errorf("expected Err to be %v, got %v", sampleErr, d.Err)
				}
			},
		},

		// 3. Allocating transitions
		{
			name:          "Allocating -> Downloading on Allocated",
			initialState:  domain.StateAllocating,
			event:         domain.Allocated{},
			expectedState: domain.StateDownloading,
		},
		{
			name:          "Allocating -> Failed on Fatal",
			initialState:  domain.StateAllocating,
			event:         domain.Fatal{Err: sampleErr},
			expectedState: domain.StateFailed,
		},

		// 4. Downloading transitions
		{
			name:          "Downloading -> Paused on PauseRequested",
			initialState:  domain.StateDownloading,
			event:         domain.PauseRequested{},
			expectedState: domain.StatePaused,
		},
		{
			name:          "Downloading -> Paused on Paused",
			initialState:  domain.StateDownloading,
			event:         domain.Paused{},
			expectedState: domain.StatePaused,
		},
		{
			name:          "Downloading -> Verifying on AllDone",
			initialState:  domain.StateDownloading,
			event:         domain.AllDone{},
			expectedState: domain.StateVerifying,
		},
		{
			name:          "Downloading -> Failed on Fatal",
			initialState:  domain.StateDownloading,
			event:         domain.Fatal{Err: sampleErr},
			expectedState: domain.StateFailed,
		},

		// 5. Paused transitions
		{
			name:          "Paused -> Queued on Resume",
			initialState:  domain.StatePaused,
			event:         domain.Resume{},
			expectedState: domain.StateQueued,
		},
		{
			name:          "Paused -> Cancelled on Cancel",
			initialState:  domain.StatePaused,
			event:         domain.Cancel{},
			expectedState: domain.StateCancelled,
		},
		{
			name:          "Paused -> Probing on RefreshLink",
			initialState:  domain.StatePaused,
			event:         domain.RefreshLink{},
			expectedState: domain.StateProbing,
		},
		{
			name:          "Paused -> Failed on Fatal",
			initialState:  domain.StatePaused,
			event:         domain.Fatal{Err: sampleErr},
			expectedState: domain.StateFailed,
		},

		// 6. Verifying transitions
		{
			name:          "Verifying -> Completed on VerifyOK",
			initialState:  domain.StateVerifying,
			event:         domain.VerifyOK{},
			expectedState: domain.StateCompleted,
		},
		{
			name:          "Verifying -> Failed on Fatal",
			initialState:  domain.StateVerifying,
			event:         domain.Fatal{Err: sampleErr},
			expectedState: domain.StateFailed,
		},

		// 7. Failed transitions
		{
			name:          "Failed -> Queued on Retry",
			initialState:  domain.StateFailed,
			event:         domain.Retry{},
			expectedState: domain.StateQueued,
			checkError: func(t *testing.T, d *domain.Download) {
				if d.Err != nil {
					t.Errorf("expected Err to be reset to nil, got %v", d.Err)
				}
			},
		},
		{
			name:          "Failed -> Probing on RefreshLink",
			initialState:  domain.StateFailed,
			event:         domain.RefreshLink{},
			expectedState: domain.StateProbing,
			checkError: func(t *testing.T, d *domain.Download) {
				if d.Err != nil {
					t.Errorf("expected Err to be reset to nil, got %v", d.Err)
				}
			},
		},
		{
			name:          "Failed -> Cancelled on Cancel",
			initialState:  domain.StateFailed,
			event:         domain.Cancel{},
			expectedState: domain.StateCancelled,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := &domain.Download{
				State: tc.initialState,
			}
			if tc.initialState == domain.StateFailed {
				d.Err = sampleErr
			}

			err := d.Transition(tc.event)
			if err != nil {
				t.Fatalf("unexpected transition error: %v", err)
			}
			if d.State != tc.expectedState {
				t.Errorf("got state %q, want %q", d.State, tc.expectedState)
			}
			if tc.checkError != nil {
				tc.checkError(t, d)
			}
		})
	}
}

func TestStateTransitions_Illegal(t *testing.T) {
	fatalErr := domain.NewDownloadError(domain.ErrCodeNotFound, "file not found", false)

	// Test at least 20 illegal state-event combinations
	illegalTests := []struct {
		state domain.State
		event domain.Event
	}{
		// From Queued
		{domain.StateQueued, domain.VerifyOK{}},
		{domain.StateQueued, domain.AllDone{}},
		{domain.StateQueued, domain.ProbeOK{Resume: false}},
		{domain.StateQueued, domain.Allocated{}},
		{domain.StateQueued, domain.Resume{}},

		// From Probing
		{domain.StateProbing, domain.SlotGranted{}},
		{domain.StateProbing, domain.AllDone{}},
		{domain.StateProbing, domain.VerifyOK{}},
		{domain.StateProbing, domain.Resume{}},

		// From Allocating
		{domain.StateAllocating, domain.SlotGranted{}},
		{domain.StateAllocating, domain.ProbeOK{Resume: false}},
		{domain.StateAllocating, domain.AllDone{}},
		{domain.StateAllocating, domain.VerifyOK{}},

		// From Downloading
		{domain.StateDownloading, domain.SlotGranted{}},
		{domain.StateDownloading, domain.ProbeOK{Resume: false}},
		{domain.StateDownloading, domain.VerifyOK{}},
		{domain.StateDownloading, domain.Resume{}},

		// From Paused
		{domain.StatePaused, domain.SlotGranted{}},
		{domain.StatePaused, domain.ProbeOK{Resume: false}},
		{domain.StatePaused, domain.Allocated{}},
		{domain.StatePaused, domain.AllDone{}},
		{domain.StatePaused, domain.VerifyOK{}},

		// From Verifying
		{domain.StateVerifying, domain.SlotGranted{}},
		{domain.StateVerifying, domain.Allocated{}},
		{domain.StateVerifying, domain.Resume{}},
		{domain.StateVerifying, domain.AllDone{}},

		// From Completed (Terminal)
		{domain.StateCompleted, domain.SlotGranted{}},
		{domain.StateCompleted, domain.Fatal{Err: fatalErr}},
		{domain.StateCompleted, domain.Cancel{}},
		{domain.StateCompleted, domain.Resume{}},

		// From Cancelled (Terminal)
		{domain.StateCancelled, domain.Resume{}},
		{domain.StateCancelled, domain.Retry{}},
		{domain.StateCancelled, domain.Fatal{Err: fatalErr}},
		{domain.StateCancelled, domain.SlotGranted{}},
	}

	for _, tc := range illegalTests {
		name := string(tc.state) + "_with_" + string(tc.state)
		t.Run(name, func(t *testing.T) {
			d := &domain.Download{State: tc.state}
			err := d.Transition(tc.event)
			if err == nil {
				t.Fatalf("expected illegal transition error from %s with event %T, got nil", tc.state, tc.event)
			}
			if !errors.Is(err, domain.ErrIllegalTransition) {
				t.Errorf("expected error wrapping ErrIllegalTransition, got: %v", err)
			}
		})
	}
}

func TestNilDownloadTransition(t *testing.T) {
	var d *domain.Download
	err := d.Transition(domain.SlotGranted{})
	if err == nil {
		t.Fatal("expected error for nil download transition, got nil")
	}
}
