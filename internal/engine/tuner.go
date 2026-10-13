package engine

import (
	"time"
)

// ActionKind represents the action selected by the tuning controller.
type ActionKind int

const (
	// ActionHold maintains current connection target.
	ActionHold ActionKind = iota
	// ActionInc increments connection target by 1.
	ActionInc
	// ActionDec decrements connection target by 1.
	ActionDec
	// ActionHalveAndWait cuts connection target in half and waits for a backoff duration.
	ActionHalveAndWait
)

// Action carries the decision made by the tuning controller.
type Action struct {
	Kind        ActionKind
	TargetConns int
	Wait        time.Duration
}

// ErrWindow summarizes network errors and throttling signals observed within an evaluation cycle.
type ErrWindow struct {
	Resets    int
	Timeouts  int
	Status429 int
	Status503 int
}

// Total returns the aggregate count of connection and throttle errors.
func (e ErrWindow) Total() int {
	return e.Resets + e.Timeouts + e.Status429 + e.Status503
}

// Sample packages observations collected from the download job for an evaluation cycle.
type Sample struct {
	ThroughputEMA float64
	Conns         int
	ErrWindow     ErrWindow
	RetryAfter    time.Duration
}

// TunerConfig configures thresholds and bounds for the connection tuning controller.
type TunerConfig struct {
	MinConns         int
	MaxConnsPerJob   int
	HardCap          int
	CooldownDuration time.Duration
}

// DefaultTunerConfig returns default parameters specified in TECH_SPEC §6.10.
func DefaultTunerConfig() TunerConfig {
	return TunerConfig{
		MinConns:         1,
		MaxConnsPerJob:   16,
		HardCap:          32,
		CooldownDuration: 30 * time.Second,
	}
}

// TunerState holds mutable state across evaluation cycles of the pure Decide function.
type TunerState struct {
	Config         TunerConfig
	LastThroughput float64
	LastConns      int
	CooldownUntil  time.Time
	LastAction     ActionKind
}

// NewTunerState initializes a TunerState with the given configuration and starting connection count.
func NewTunerState(cfg TunerConfig, initialConns int) TunerState {
	if cfg.MinConns <= 0 {
		cfg.MinConns = 1
	}
	if cfg.MaxConnsPerJob <= 0 {
		cfg.MaxConnsPerJob = 16
	}
	if cfg.HardCap <= 0 {
		cfg.HardCap = 32
	}
	if cfg.CooldownDuration <= 0 {
		cfg.CooldownDuration = 30 * time.Second
	}
	if initialConns < cfg.MinConns {
		initialConns = cfg.MinConns
	}

	return TunerState{
		Config:    cfg,
		LastConns: initialConns,
	}
}

// Decide is a pure function mapping previous state and new observation sample to next state and action.
func Decide(now time.Time, state TunerState, s Sample) (TunerState, Action) {
	cfg := state.Config
	maxAllowed := cfg.MaxConnsPerJob
	if cfg.HardCap > 0 && maxAllowed > cfg.HardCap {
		maxAllowed = cfg.HardCap
	}
	if maxAllowed < cfg.MinConns {
		maxAllowed = cfg.MinConns
	}

	curConns := s.Conns
	if curConns < cfg.MinConns {
		curConns = cfg.MinConns
	}

	if s.ErrWindow.Status429 > 0 || s.ErrWindow.Status503 > 0 || s.ErrWindow.Total() >= 3 {
		target := curConns / 2
		if target < cfg.MinConns {
			target = cfg.MinConns
		}

		waitDur := s.RetryAfter
		if waitDur <= 0 {
			waitDur = 1 * time.Second
		}
		cooldownDur := cfg.CooldownDuration
		if waitDur > cooldownDur {
			cooldownDur = waitDur
		}

		state.CooldownUntil = now.Add(cooldownDur)
		state.LastThroughput = s.ThroughputEMA
		state.LastConns = target
		state.LastAction = ActionHalveAndWait

		return state, Action{
			Kind:        ActionHalveAndWait,
			TargetConns: target,
			Wait:        waitDur,
		}
	}

	if !state.CooldownUntil.IsZero() && now.Before(state.CooldownUntil) {
		return state, Action{
			Kind:        ActionHold,
			TargetConns: curConns,
		}
	}

	if state.LastThroughput <= 0 {
		state.LastThroughput = s.ThroughputEMA
		state.LastConns = curConns
		state.LastAction = ActionHold
		return state, Action{
			Kind:        ActionHold,
			TargetConns: curConns,
		}
	}

	if state.LastAction == ActionInc {
		if s.ThroughputEMA < state.LastThroughput*0.85 {
			target := curConns - 1
			if target < cfg.MinConns {
				target = cfg.MinConns
			}

			state.CooldownUntil = now.Add(cfg.CooldownDuration)
			state.LastThroughput = s.ThroughputEMA
			state.LastConns = target
			state.LastAction = ActionDec

			return state, Action{
				Kind:        ActionDec,
				TargetConns: target,
			}
		}
	}

	if s.ThroughputEMA > state.LastThroughput*1.08 {
		if curConns < maxAllowed {
			target := curConns + 1
			state.LastThroughput = s.ThroughputEMA
			state.LastConns = target
			state.LastAction = ActionInc

			return state, Action{
				Kind:        ActionInc,
				TargetConns: target,
			}
		}
	}

	state.LastThroughput = s.ThroughputEMA
	state.LastConns = curConns
	state.LastAction = ActionHold

	return state, Action{
		Kind:        ActionHold,
		TargetConns: curConns,
	}
}
