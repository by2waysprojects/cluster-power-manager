package scaling

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type updaterMock struct {
	mock.Mock
}

func (u *updaterMock) Update(opts *CPUScalingOpts) time.Duration {
	return u.Called(opts).Get(0).(time.Duration)
}

type MockDPDKTelemetryClient struct {
	mock.Mock
	DPDKTelemetryClient
}

func (md *MockDPDKTelemetryClient) GetUsagePercent(cpuID uint) (int, error) {
	return md.Called().Get(0).(int), md.Called().Error(1)
}

func (md *MockDPDKTelemetryClient) GetCPUClass(cpuID uint) CPUClass {
	return md.Called(cpuID).Get(0).(CPUClass)
}

func TestCPUScalingUpdater_Update(t *testing.T) {
	tCases := []struct {
		testCase     string
		cpuID        uint
		cpuClass     CPUClass
		scalingOpts  *CPUScalingOpts
		currentUsage int
		usageErr     error
		currentFreq  string
		expectedFreq string
		nextSetIn    time.Duration
	}{
		{
			testCase: "Test Case 1 - New frequency is set - 1",
			cpuID:    0,
			cpuClass: CPUClassPollingLcore,
			scalingOpts: &CPUScalingOpts{
				TargetUsage:                80,
				AllowedUsageDifference:     5,
				SamplePeriod:               10 * time.Millisecond,
				CurrentTargetFrequency:     1340980,
				FallbackFreq:               2000000,
				HWMaxFrequency:             3700000,
				HWMinFrequency:             400000,
				ScaleFactor:                1.0,
				AllowedFrequencyDifference: 10000,
				CooldownPeriod:             20 * time.Millisecond,
			},
			currentUsage: 46,
			usageErr:     nil,
			currentFreq:  "1340980",
			expectedFreq: "771063",
			nextSetIn:    20 * time.Millisecond,
		},
		{
			testCase: "Test Case 1 - New frequency is set - 2",
			cpuID:    1,
			cpuClass: CPUClassPollingLcore,
			scalingOpts: &CPUScalingOpts{
				TargetUsage:                41,
				AllowedUsageDifference:     3,
				SamplePeriod:               10 * time.Millisecond,
				FallbackFreq:               2000000,
				CurrentTargetFrequency:     2134003,
				HWMaxFrequency:             3200000,
				HWMinFrequency:             1000000,
				ScaleFactor:                0.5,
				AllowedFrequencyDifference: 10000,
				CooldownPeriod:             35 * time.Millisecond,
			},
			currentUsage: 74,
			usageErr:     nil,
			currentFreq:  "2134003",
			expectedFreq: "2992809",
			nextSetIn:    35 * time.Millisecond,
		},
		{
			testCase: "Test Case 1 - New frequency is set - 3",
			cpuID:    1,
			cpuClass: CPUClassPollingLcore,
			scalingOpts: &CPUScalingOpts{
				TargetUsage:                58,
				AllowedUsageDifference:     3,
				SamplePeriod:               10 * time.Millisecond,
				FallbackFreq:               2000000,
				CurrentTargetFrequency:     3024616,
				HWMaxFrequency:             3700000,
				HWMinFrequency:             400000,
				ScaleFactor:                1.9,
				AllowedFrequencyDifference: 10000,
				CooldownPeriod:             35 * time.Millisecond,
			},
			currentUsage: 52,
			usageErr:     nil,
			currentFreq:  "3024616",
			expectedFreq: "2430122",
			nextSetIn:    35 * time.Millisecond,
		},
		{
			testCase: "Test Case 1 - New frequency is set - 4, capped to HWMax",
			cpuID:    1,
			cpuClass: CPUClassPollingLcore,
			scalingOpts: &CPUScalingOpts{
				TargetUsage:                60,
				AllowedUsageDifference:     5,
				SamplePeriod:               10 * time.Millisecond,
				FallbackFreq:               2000000,
				CurrentTargetFrequency:     2844827,
				HWMaxFrequency:             3200000,
				HWMinFrequency:             1000000,
				ScaleFactor:                1.6,
				AllowedFrequencyDifference: 10000,
				CooldownPeriod:             20 * time.Millisecond,
			},
			currentUsage: 66,
			usageErr:     nil,
			currentFreq:  "2844827",
			expectedFreq: "3200000",
			nextSetIn:    20 * time.Millisecond,
		},
		{
			testCase: "Test Case 1 - New frequency is set - 5, capped to HWMin",
			cpuID:    1,
			cpuClass: CPUClassPollingLcore,
			scalingOpts: &CPUScalingOpts{
				TargetUsage:                70,
				AllowedUsageDifference:     5,
				SamplePeriod:               10 * time.Millisecond,
				FallbackFreq:               2000000,
				CurrentTargetFrequency:     1260000,
				HWMaxFrequency:             3200000,
				HWMinFrequency:             1000000,
				ScaleFactor:                0.75,
				AllowedFrequencyDifference: 10000,
				CooldownPeriod:             20 * time.Millisecond,
			},
			currentUsage: 50,
			usageErr:     nil,
			currentFreq:  "1260000",
			expectedFreq: "1000000",
			nextSetIn:    20 * time.Millisecond,
		},
		{
			testCase: "Test Case 2 - Usage difference is within accepted range",
			cpuID:    2,
			cpuClass: CPUClassPollingLcore,
			scalingOpts: &CPUScalingOpts{
				TargetUsage:                80,
				AllowedUsageDifference:     5,
				SamplePeriod:               10 * time.Millisecond,
				FallbackFreq:               2000000,
				CurrentTargetFrequency:     2500000,
				HWMaxFrequency:             3700000,
				HWMinFrequency:             400000,
				ScaleFactor:                1.0,
				AllowedFrequencyDifference: 10000,
				CooldownPeriod:             20 * time.Millisecond,
			},
			currentUsage: 82,
			usageErr:     nil,
			currentFreq:  "2500000",
			expectedFreq: "",
			nextSetIn:    10 * time.Millisecond,
		},
		{
			testCase: "Test Case 3 - Frequency difference is within accepted range",
			cpuID:    2,
			cpuClass: CPUClassPollingLcore,
			scalingOpts: &CPUScalingOpts{
				TargetUsage:                80,
				AllowedUsageDifference:     1,
				SamplePeriod:               15 * time.Millisecond,
				FallbackFreq:               2000000,
				CurrentTargetFrequency:     3418600,
				HWMaxFrequency:             3700000,
				HWMinFrequency:             400000,
				ScaleFactor:                1.0,
				AllowedFrequencyDifference: 100000,
				CooldownPeriod:             20 * time.Millisecond,
			},
			currentUsage: 78,
			usageErr:     nil,
			currentFreq:  "3607692",
			expectedFreq: "",
			nextSetIn:    15 * time.Millisecond,
		},
		{
			testCase: "Test Case 4 - Current frequency is not yet set",
			cpuID:    2,
			cpuClass: CPUClassPollingLcore,
			scalingOpts: &CPUScalingOpts{
				TargetUsage:                80,
				AllowedUsageDifference:     1,
				SamplePeriod:               15 * time.Millisecond,
				FallbackFreq:               2000000,
				CurrentTargetFrequency:     FrequencyNotYetSet,
				HWMaxFrequency:             3700000,
				HWMinFrequency:             400000,
				ScaleFactor:                1.0,
				AllowedFrequencyDifference: 100000,
				CooldownPeriod:             20 * time.Millisecond,
			},
			currentUsage: 78,
			usageErr:     nil,
			currentFreq:  "3607692",
			expectedFreq: "3517499",
			nextSetIn:    20 * time.Millisecond,
		},
		{
			testCase: "Test Case 5 - Error getting usage",
			cpuID:    2,
			cpuClass: CPUClassPollingLcore,
			scalingOpts: &CPUScalingOpts{
				TargetUsage:                80,
				AllowedUsageDifference:     1,
				SamplePeriod:               10 * time.Millisecond,
				FallbackFreq:               2000000,
				CurrentTargetFrequency:     3512500,
				HWMaxFrequency:             3700000,
				HWMinFrequency:             400000,
				ScaleFactor:                1.0,
				AllowedFrequencyDifference: 100000,
				CooldownPeriod:             20 * time.Millisecond,
			},
			currentUsage: 0,
			usageErr:     ErrDPDKMetricMissing,
			currentFreq:  "3512500",
			expectedFreq: "2000000",
			nextSetIn:    10 * time.Millisecond,
		},
		{
			testCase: "Test Case 6 - Error getting usage, fallback frequency is already set",
			cpuID:    2,
			cpuClass: CPUClassPollingLcore,
			scalingOpts: &CPUScalingOpts{
				TargetUsage:                80,
				AllowedUsageDifference:     1,
				SamplePeriod:               10 * time.Millisecond,
				FallbackFreq:               2000000,
				CurrentTargetFrequency:     2000000,
				HWMaxFrequency:             3700000,
				HWMinFrequency:             400000,
				AllowedFrequencyDifference: 100000,
				ScaleFactor:                1.0,
				CooldownPeriod:             20 * time.Millisecond,
			},
			currentUsage: 0,
			usageErr:     ErrDPDKMetricMissing,
			currentFreq:  "2000000",
			expectedFreq: "",
			nextSetIn:    10 * time.Millisecond,
		},
		{
			testCase: "Test Case 7 - Error getting current frequency",
			cpuID:    2,
			cpuClass: CPUClassPollingLcore,
			scalingOpts: &CPUScalingOpts{
				TargetUsage:                80,
				AllowedUsageDifference:     1,
				SamplePeriod:               10 * time.Millisecond,
				FallbackFreq:               2000000,
				CurrentTargetFrequency:     3512500,
				HWMaxFrequency:             3700000,
				HWMinFrequency:             400000,
				AllowedFrequencyDifference: 100000,
				ScaleFactor:                1.0,
				CooldownPeriod:             20 * time.Millisecond,
			},
			currentUsage: 70,
			usageErr:     nil,
			currentFreq:  "not-a-frequency",
			expectedFreq: "2000000",
			nextSetIn:    10 * time.Millisecond,
		},
		{
			testCase: "Test Case 8 - Error getting current frequency, fallback frequency is already set",
			cpuID:    2,
			cpuClass: CPUClassPollingLcore,
			scalingOpts: &CPUScalingOpts{
				TargetUsage:                80,
				AllowedUsageDifference:     1,
				SamplePeriod:               10 * time.Millisecond,
				FallbackFreq:               2000000,
				CurrentTargetFrequency:     2000000,
				HWMaxFrequency:             3700000,
				HWMinFrequency:             400000,
				AllowedFrequencyDifference: 100000,
				ScaleFactor:                1.0,
				CooldownPeriod:             20 * time.Millisecond,
			},
			currentUsage: 70,
			usageErr:     nil,
			currentFreq:  "not-a-frequency",
			expectedFreq: "",
			nextSetIn:    10 * time.Millisecond,
		},
	}

	for _, tc := range tCases {
		t.Run(tc.testCase, func(t *testing.T) {
			// Setup mock filesystem with userspace governor
			host, teardown, err := setupScalingTestFiles(3, map[string]string{
				"governor": "userspace",
				"max":      fmt.Sprintf("%d", tc.scalingOpts.HWMaxFrequency),
				"min":      fmt.Sprintf("%d", tc.scalingOpts.HWMinFrequency),
			})
			assert.NoError(t, err)
			defer teardown()

			cpu := host.GetAllCpus().ByID(tc.cpuID)
			tc.scalingOpts.CPU = cpu

			// Prepare current frequency file and setspeed file paths
			curFreqPath := filepath.Join("testing", "cpus", fmt.Sprintf("cpu%d", tc.cpuID), "cpufreq", "scaling_cur_freq")
			setFreqpath := filepath.Join("testing", "cpus", fmt.Sprintf("cpu%d", tc.cpuID), "cpufreq", "scaling_setspeed")

			// Set current frequency and clear setspeed file
			err = os.WriteFile(curFreqPath, []byte(tc.currentFreq+"\n"), 0o644)
			assert.NoError(t, err)
			err = os.WriteFile(setFreqpath, []byte(""), 0o644)
			assert.NoError(t, err)

			dpdkmock := &MockDPDKTelemetryClient{}
			dpdkmock.On("GetCPUClass", tc.cpuID).Return(tc.cpuClass)
			dpdkmock.On("GetUsagePercent").Return(tc.currentUsage, tc.usageErr)

			// Run updater to set the frequency
			updater := &cpuScalingUpdaterImpl{dpdkClient: dpdkmock}
			nextSetIn := updater.Update(tc.scalingOpts)

			// Verify setspeed matches expected frequency
			frequencyRaw, err := os.ReadFile(setFreqpath)
			assert.NoError(t, err)
			frequency := strings.Trim(string(frequencyRaw), " \n")
			assert.Equal(t, tc.expectedFreq, frequency)
			assert.Equal(t, tc.nextSetIn, nextSetIn)

			dpdkmock.AssertExpectations(t)
		})
	}
}

// TestCPUScalingUpdater_Update_NonPollingClasses covers the CPUs of a DPDK
// container that are not polling lcores. These must be held at a fixed
// frequency rather than left at the fallback frequency, which would otherwise
// put a floor under the physical core shared with a polling lcore.
func TestCPUScalingUpdater_Update_NonPollingClasses(t *testing.T) {
	const (
		hwMin   = 400000
		hwMax   = 3700000
		appFreq = 3700000
	)

	tCases := []struct {
		testCase       string
		cpuClass       CPUClass
		siblingIDs     []uint
		siblingClass   CPUClass
		expectedFreq   string
		expectedTarget int
	}{
		{
			testCase:       "Silent lcore is held at the app frequency",
			cpuClass:       CPUClassSilentLcore,
			expectedFreq:   fmt.Sprintf("%d", appFreq),
			expectedTarget: appFreq,
		},
		{
			testCase:       "Non-lcore sharing a core with an lcore is held at the hardware minimum",
			cpuClass:       CPUClassNonLcore,
			siblingIDs:     []uint{1},
			siblingClass:   CPUClassPollingLcore,
			expectedFreq:   fmt.Sprintf("%d", hwMin),
			expectedTarget: hwMin,
		},
		{
			testCase:       "Non-lcore sharing a core with a silent lcore is held at the hardware minimum",
			cpuClass:       CPUClassNonLcore,
			siblingIDs:     []uint{1},
			siblingClass:   CPUClassSilentLcore,
			expectedFreq:   fmt.Sprintf("%d", hwMin),
			expectedTarget: hwMin,
		},
		{
			testCase:       "Non-lcore whose sibling is also not an lcore is held at the app frequency",
			cpuClass:       CPUClassNonLcore,
			siblingIDs:     []uint{1},
			siblingClass:   CPUClassNonLcore,
			expectedFreq:   fmt.Sprintf("%d", appFreq),
			expectedTarget: appFreq,
		},
		{
			// Hyper-Threading disabled: the CPU has a physical core to itself,
			// so there is no sibling that could be floored by it.
			testCase:       "Non-lcore with no siblings at all is held at the app frequency",
			cpuClass:       CPUClassNonLcore,
			siblingIDs:     nil,
			expectedFreq:   fmt.Sprintf("%d", appFreq),
			expectedTarget: appFreq,
		},
		{
			// An application predating the lcore telemetry endpoints cannot be
			// classified at all, so none of its CPUs can be scaled.
			testCase:       "Unsupported CPU is held at the app frequency",
			cpuClass:       CPUClassUnsupported,
			expectedFreq:   fmt.Sprintf("%d", appFreq),
			expectedTarget: appFreq,
		},
		{
			// An unsupported sibling is not known to be an lcore, and is held
			// at the app frequency anyway, so flooring this CPU would only
			// starve it without letting the core drop any lower.
			testCase:       "Non-lcore sharing a core with an unsupported CPU is held at the app frequency",
			cpuClass:       CPUClassNonLcore,
			siblingIDs:     []uint{1},
			siblingClass:   CPUClassUnsupported,
			expectedFreq:   fmt.Sprintf("%d", appFreq),
			expectedTarget: appFreq,
		},
		{
			testCase:       "Unclassified CPU falls back until discovery completes",
			cpuClass:       CPUClassUnknown,
			expectedFreq:   "2000000",
			expectedTarget: 2000000,
		},
	}

	for _, tc := range tCases {
		t.Run(tc.testCase, func(t *testing.T) {
			host, teardown, err := setupScalingTestFiles(3, map[string]string{
				"governor": "userspace",
				"max":      fmt.Sprintf("%d", hwMax),
				"min":      fmt.Sprintf("%d", hwMin),
			})
			assert.NoError(t, err)
			defer teardown()

			const cpuID uint = 0
			opts := &CPUScalingOpts{
				CPU:                    host.GetAllCpus().ByID(cpuID),
				SamplePeriod:           10 * time.Millisecond,
				CooldownPeriod:         20 * time.Millisecond,
				HWMaxFrequency:         hwMax,
				HWMinFrequency:         hwMin,
				CurrentTargetFrequency: FrequencyNotYetSet,
				FallbackFreq:           2000000,
				AppFreq:                appFreq,
				SiblingCPUIDs:          tc.siblingIDs,
			}

			setFreqpath := filepath.Join("testing", "cpus", fmt.Sprintf("cpu%d", cpuID), "cpufreq", "scaling_setspeed")
			assert.NoError(t, os.WriteFile(setFreqpath, []byte(""), 0o644))

			dpdkmock := &MockDPDKTelemetryClient{}
			dpdkmock.On("GetCPUClass", cpuID).Return(tc.cpuClass)
			for _, siblingID := range tc.siblingIDs {
				dpdkmock.On("GetCPUClass", siblingID).Return(tc.siblingClass)
			}

			updater := &cpuScalingUpdaterImpl{dpdkClient: dpdkmock}
			nextSetIn := updater.Update(opts)

			frequencyRaw, err := os.ReadFile(setFreqpath)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedFreq, strings.Trim(string(frequencyRaw), " \n"))
			assert.Equal(t, tc.expectedTarget, opts.CurrentTargetFrequency)
			assert.Equal(t, opts.SamplePeriod, nextSetIn)

			// A held frequency is written once; the next tick is a no-op.
			assert.NoError(t, os.WriteFile(setFreqpath, []byte(""), 0o644))
			updater.Update(opts)
			frequencyRaw, err = os.ReadFile(setFreqpath)
			assert.NoError(t, err)
			assert.Empty(t, strings.Trim(string(frequencyRaw), " \n"))

			dpdkmock.AssertExpectations(t)
		})
	}
}
