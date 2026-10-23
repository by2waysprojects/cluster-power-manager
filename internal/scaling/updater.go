package scaling

import (
	"time"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"
)

type CPUScalingUpdater interface {
	Update(opts *CPUScalingOpts) time.Duration
}

type cpuScalingUpdaterImpl struct {
	dpdkClient DPDKTelemetryClient
	logger     logr.Logger
}

func NewCPUScalingUpdater(dpdkClient DPDKTelemetryClient) CPUScalingUpdater {
	updater := &cpuScalingUpdaterImpl{
		dpdkClient: dpdkClient,
		logger:     ctrl.Log.WithName("CPUScalingUpdater"),
	}

	return updater
}

// Update drives the managed CPU towards the frequency its role calls for, and
// returns the duration until the next update (cooldown or sample period).
//
// Only a DPDK lcore that reports usage is scaled dynamically. Every other CPU
// of the container is held at a fixed frequency, so that it does not put a
// frequency floor under the physical core it may share with an lcore.
func (u *cpuScalingUpdaterImpl) Update(opts *CPUScalingOpts) time.Duration {
	switch u.dpdkClient.GetCPUClass(opts.CPU.GetID()) {
	case CPUClassPollingLcore:
		return u.scale(opts)
	case CPUClassSilentLcore, CPUClassUnsupported:
		// Either a real lcore that never reports usage, typically the main
		// lcore, or a CPU of an application CPM cannot classify at all. Either
		// way it runs work CPM cannot size, so it is treated as an app CPU.
		return u.hold(opts, opts.AppFreq, "app")
	case CPUClassNonLcore:
		if u.hasLcoreSibling(opts) {
			return u.hold(opts, opts.HWMinFrequency, "hardware minimum")
		}
		return u.hold(opts, opts.AppFreq, "app")
	default:
		// Not classified yet: telemetry is not connected, or the first
		// discovery pass has not completed. This is the transient case the
		// fallback frequency exists for.
		return u.hold(opts, opts.FallbackFreq, "fallback")
	}
}

// hasLcoreSibling reports whether any CPU of the same physical core is a DPDK
// lcore. A sibling that is unclassified, or that belongs to an application CPM
// cannot classify, does not count: it is not known to be an lcore, and
// flooring this CPU on the strength of a guess would starve it for nothing.
func (u *cpuScalingUpdaterImpl) hasLcoreSibling(opts *CPUScalingOpts) bool {
	for _, siblingID := range opts.SiblingCPUIDs {
		switch u.dpdkClient.GetCPUClass(siblingID) {
		case CPUClassPollingLcore, CPUClassSilentLcore:
			return true
		}
	}

	return false
}

// hold pins the CPU to a fixed frequency. Once applied, subsequent calls are a
// no-op until the CPU's class, and with it the target frequency, changes.
func (u *cpuScalingUpdaterImpl) hold(opts *CPUScalingOpts, frequency int, reason string) time.Duration {
	if frequencyInAllowedDifference(frequency, opts) {
		return opts.SamplePeriod
	}

	if err := opts.CPU.SetCPUFrequency(uint(frequency)); err != nil {
		u.logger.Error(err, "failed to set frequency", "cpu", opts.CPU.GetID(), "freq", frequency, "reason", reason)
		return opts.SamplePeriod
	}
	opts.CurrentTargetFrequency = frequency
	u.logger.V(6).Info("holding frequency", "cpu", opts.CPU.GetID(), "freq", frequency, "reason", reason)

	return opts.SamplePeriod
}

// scale adjusts the frequency of a DPDK lcore to keep its usage near the target.
func (u *cpuScalingUpdaterImpl) scale(opts *CPUScalingOpts) time.Duration {
	currentUsage, err := u.dpdkClient.GetUsagePercent(opts.CPU.GetID())
	if err != nil {
		// No usable sample this tick (startup, reconnect, counter reset or an
		// inconsistent sample). Fall back until a sample arrives.
		return u.hold(opts, opts.FallbackFreq, "fallback")
	}

	if currentUsage >= opts.TargetUsage-opts.AllowedUsageDifference &&
		currentUsage <= opts.TargetUsage+opts.AllowedUsageDifference {
		return opts.SamplePeriod
	}

	currentFrequencyUint, err := opts.CPU.GetCurrentCPUFrequency()
	if err != nil {
		return u.hold(opts, opts.FallbackFreq, "fallback")
	}
	currentFrequency := int(currentFrequencyUint)

	nextFrequencyFloat :=
		float64(currentFrequency) * (1.0 + (float64(currentUsage)/float64(opts.TargetUsage)-1.0)*opts.ScaleFactor)
	nextFrequency := int(nextFrequencyFloat)

	if nextFrequency < opts.HWMinFrequency {
		nextFrequency = opts.HWMinFrequency
	}
	if nextFrequency > opts.HWMaxFrequency {
		nextFrequency = opts.HWMaxFrequency
	}

	if frequencyInAllowedDifference(nextFrequency, opts) {
		return opts.SamplePeriod
	}

	err = opts.CPU.SetCPUFrequency(uint(nextFrequency))
	if err != nil {
		u.logger.Error(err, "failed to set next frequency", "cpu", opts.CPU.GetID(), "next_freq", nextFrequency)
		return opts.SamplePeriod
	}
	opts.CurrentTargetFrequency = nextFrequency
	u.logger.V(6).Info("set next frequency",
		"cpu", opts.CPU.GetID(),
		"usage", currentUsage,
		"prev_freq", currentFrequency,
		"next_freq", nextFrequency,
	)

	return opts.CooldownPeriod
}

func frequencyInAllowedDifference(frequency int, opts *CPUScalingOpts) bool {
	if opts.CurrentTargetFrequency != FrequencyNotYetSet &&
		frequency >= opts.CurrentTargetFrequency-opts.AllowedFrequencyDifference &&
		frequency <= opts.CurrentTargetFrequency+opts.AllowedFrequencyDifference {
		return true
	}

	return false
}
