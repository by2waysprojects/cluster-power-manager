package scaling

import (
	"time"

	"github.com/cluster-power-manager/cluster-power-manager/internal/power"
)

const FrequencyNotYetSet int = -1

type CPUScalingOpts struct {
	CPU                        power.CPU
	SamplePeriod               time.Duration
	CooldownPeriod             time.Duration
	TargetUsage                int
	AllowedUsageDifference     int
	AllowedFrequencyDifference int
	HWMaxFrequency             int
	HWMinFrequency             int
	CurrentTargetFrequency     int
	ScaleFactor                float64
	FallbackFreq               int
	// AppFreq is applied to CPUs of the container that are not DPDK polling
	// lcores and do not share a physical core with an lcore.
	AppFreq int
	// SiblingCPUIDs are the other CPUs of the same physical core. A CPU that is
	// not an lcore but shares a core with one is held at the hardware minimum,
	// so that it does not put a frequency floor under its lcore sibling.
	SiblingCPUIDs []uint
}
