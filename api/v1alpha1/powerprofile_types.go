/*


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// PowerProfileSpec defines the desired state of PowerProfile
// +kubebuilder:validation:XValidation:rule="!has(self.cpuScalingPolicy) || (has(self.pstates.governor) && self.pstates.governor == 'userspace')",message="pstates.governor must be 'userspace' when cpuScalingPolicy is set"
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.cpuScalingPolicy) || has(self.cpuScalingPolicy)",message="cpuScalingPolicy cannot be removed once set"
type PowerProfileSpec struct {
	// Important: Run "make" to regenerate code after modifying this file

	Shared bool `json:"shared,omitempty"`

	// NodeSelector specifies which nodes this PowerProfile should be applied to
	// If empty, the profile will be applied to all the nodes.
	// +optional
	NodeSelector NodeSelector `json:"nodeSelector,omitempty"`

	// P-states configuration
	PStates PStatesConfig `json:"pstates,omitempty"`

	// C-states configuration
	CStates CStatesConfig `json:"cstates,omitempty"`

	// Defines the number or percentage of CPUs that can be allocated to this profile.
	// If not specified, it defaults to 100% of the available CPUs.
	// Accepted values are:
	// - A number (e.g., 5)
	// - A percentage (e.g., "10%")
	// +kubebuilder:validation:XIntOrString
	// +kubebuilder:validation:Pattern=`^([1-9][0-9]?|100)%?$`
	// +kubebuilder:default="100%"
	CPUCapacity intstr.IntOrString `json:"cpuCapacity,omitempty"`

	// Configures DPDK telemetry-based dynamic CPU frequency scaling.
	CPUScalingPolicy *CPUScalingPolicy `json:"cpuScalingPolicy,omitempty"`
}

type NodeSelector struct {
	// LabelSelector is a label selector that specifies which nodes this PowerProfile should be
	// applied to.
	// +optional
	LabelSelector metav1.LabelSelector `json:"labelSelector,omitempty"`
}

// PStatesConfig defines the CPU P-states configuration
type PStatesConfig struct {
	// Max frequency cores can run at. If not specified, it defaults to the maximum frequency of the CPU.
	// If specified as a percentage, the following formula is used: min + (max - min) * percentage.
	// +kubebuilder:validation:XIntOrString
	// +kubebuilder:validation:Pattern=`^(\d+|([1-9]?\d|100)%)$`
	Max *intstr.IntOrString `json:"max,omitempty"`

	// Min frequency cores can run at. If not specified, it defaults to the minimum frequency of the CPU.
	// If specified as a percentage, the following formula is used: min + (max - min) * percentage.
	// +kubebuilder:validation:XIntOrString
	// +kubebuilder:validation:Pattern=`^(\d+|([1-9]?\d|100)%)$`
	Min *intstr.IntOrString `json:"min,omitempty"`

	// The priority value associated with this Power Profile
	Epp string `json:"epp,omitempty"`

	// Governor to be used
	// +kubebuilder:default=powersave
	Governor string `json:"governor,omitempty"`
}

// CStatesConfig defines the CPU C-states configuration.
// +kubebuilder:validation:XValidation:rule="!(has(self.names) && has(self.maxLatencyUs))",message="Specify either 'names' or 'maxLatencyUs' for C-state configuration, but not both"
type CStatesConfig struct {
	// Names defines explicit C-state configuration.
	// The map key represents the C-state name (e.g., "C1", "C1E", "C6" etc.).
	// The map value represents whether the C-state should be enabled (true) or disabled (false).
	// This field is mutually exclusive with 'maxLatencyUs' — only one of 'names' or 'maxLatencyUs' may be set.
	Names map[string]bool `json:"names,omitempty"`

	// MaxLatencyUs defines the maximum latency threshold in microseconds.
	// C-states with latency higher than this threshold will be disabled.
	// This field is mutually exclusive with 'names' — only one of 'names' or 'maxLatencyUs' may be set.
	// +kubebuilder:validation:Minimum=0
	MaxLatencyUs *int `json:"maxLatencyUs,omitempty"`
}

// CPUScalingPolicy configures DPDK telemetry-based dynamic CPU frequency scaling.
// +kubebuilder:validation:XValidation:rule="duration(self.samplePeriod).getMilliseconds() >= 10 && duration(self.samplePeriod).getMilliseconds() <= 1000",message="samplePeriod must be between 10ms and 1s"
// +kubebuilder:validation:XValidation:rule="duration(self.cooldownPeriod).getMilliseconds() >= duration(self.samplePeriod).getMilliseconds()",message="cooldownPeriod must be larger than samplePeriod"
type CPUScalingPolicy struct {
	// Workload type
	// +kubebuilder:validation:Enum=polling-dpdk
	// +kubebuilder:default=polling-dpdk
	WorkloadType string `json:"workloadType,omitempty"`

	// Time to elapse between two CPU sampling periods for scaling control.
	// At each sampling period the scaler reads CPU usage and adjusts frequency if needed.
	// +kubebuilder:validation:Format=duration
	// +kubebuilder:default="10ms"
	SamplePeriod *metav1.Duration `json:"samplePeriod,omitempty"`

	// Time to elapse after setting a new frequency target before next scaling control.
	// +kubebuilder:validation:Format=duration
	// +kubebuilder:default="30ms"
	CooldownPeriod *metav1.Duration `json:"cooldownPeriod,omitempty"`

	// Target CPU usage, in percent
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=80
	TargetUsage *int `json:"targetUsage,omitempty"`

	// Maximum difference between target and actual CPU usage on which
	// frequency re-evaluation will not happen, in percent points
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=50
	// +kubebuilder:default=5
	AllowedUsageDifference *int `json:"allowedUsageDifference,omitempty"`

	// Maximum difference between target and actual CPU frequency on which
	// frequency re-evaluation will not happen, in MHz
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=25
	AllowedFrequencyDifference *int `json:"allowedFrequencyDifference,omitempty"`

	// Percentage factor of CPU frequency change when scaling
	// +kubebuilder:validation:Minimum=10
	// +kubebuilder:validation:Maximum=200
	// +kubebuilder:default=50
	ScalePercentage *int `json:"scalePercentage,omitempty"`

	// Frequency to set when a usage sample is temporarily not available for a
	// DPDK lcore, in percent of the range between the hardware minimum and
	// maximum frequencies: 0 is the hardware minimum, 100 the hardware maximum
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=100
	FallbackFreqPercent *int `json:"fallbackFreqPercent,omitempty"`

	// Frequency to set for CPUs of the container that are not DPDK polling
	// lcores, in percent of the range between the hardware minimum and
	// maximum frequencies: 0 is the hardware minimum, 100 the hardware maximum
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=80
	AppFreqPercent *int `json:"appFreqPercent,omitempty"`
}

// PowerProfileStatus defines the observed state of PowerProfile
type PowerProfileStatus struct {
	// The ID given to the power profile
	ID int `json:"id,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// PowerProfile is the Schema for the powerprofiles API
type PowerProfile struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PowerProfileSpec   `json:"spec,omitempty"`
	Status PowerProfileStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PowerProfileList contains a list of PowerProfile
type PowerProfileList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PowerProfile `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PowerProfile{}, &PowerProfileList{})
}
