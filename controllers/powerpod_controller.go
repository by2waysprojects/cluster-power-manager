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

package controllers

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"

	e "errors"

	powerv1alpha1 "github.com/cluster-power-manager/cluster-power-manager/api/v1alpha1"
	"github.com/cluster-power-manager/cluster-power-manager/internal/power"
	"github.com/cluster-power-manager/cluster-power-manager/internal/scaling"
	"github.com/go-logr/logr"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/cluster-power-manager/cluster-power-manager/pkg/podresourcesclient"
	"github.com/cluster-power-manager/cluster-power-manager/pkg/podstate"
)

const (
	PowerProfileAnnotation  = "PowerProfile"
	ResourcePrefix          = "power.cluster-power-manager.github.io/"
	CPUResource             = "cpu"
	WorkloadTypePollingDPDK = "polling-dpdk"
	PowerNamespace          = "power-manager"
)

// PowerPodReconciler reconciles a Pod object
type PowerPodReconciler struct {
	client.Client
	Log                 logr.Logger
	Scheme              *runtime.Scheme
	State               *podstate.State
	PodResourcesClient  podresourcesclient.PodResourcesClient
	PowerLibrary        power.Host
	DPDKTelemetryClient scaling.DPDKTelemetryClient
	CPUScalingManager   scaling.CPUScalingManager
}

// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch,roleName=node-agent-role
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch,roleName=node-agent-role
// +kubebuilder:rbac:groups=power.cluster-power-manager.github.io,resources=powerprofiles,verbs=get;list;watch,roleName=node-agent-role
// +kubebuilder:rbac:groups=power.cluster-power-manager.github.io,resources=powernodestates,verbs=get;list;watch,roleName=node-agent-role
// +kubebuilder:rbac:groups=power.cluster-power-manager.github.io,resources=powernodestates/status,verbs=get;patch,roleName=node-agent-role

func (r *PowerPodReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := r.Log.WithValues("powerpod", req.NamespacedName)
	logger.Info("Reconciling the pod")

	pod := &corev1.Pod{}
	logger.V(5).Info("retrieving pod instance")
	err := r.Get(ctx, req.NamespacedName, pod)
	if err != nil {
		if errors.IsNotFound(err) {
			// Delete the Pod from the internal state in case it was never deleted
			// aAdded the check due to golangcilint errcheck
			_ = r.State.DeletePodFromState(req.NamespacedName.Name, req.NamespacedName.Namespace)
			return ctrl.Result{}, nil
		}

		logger.Error(err, "error while trying to retrieve the pod")
		return ctrl.Result{}, err
	}

	// The NODE_NAME environment variable is passed in via the downwards API in the pod spec
	nodeName := os.Getenv("NODE_NAME")

	if !pod.ObjectMeta.DeletionTimestamp.IsZero() || pod.Status.Phase == corev1.PodSucceeded {
		return r.handlePodDeletion(ctx, pod, nodeName, &logger)
	}

	// If the pod's deletion timestamp is equal to zero, then the pod has been created or updated.

	// Make sure the pod is running
	logger.V(5).Info("confirming the pod is in a running state")
	podNotRunningErr := errors.NewServiceUnavailable("the pod is not in the running phase")
	if pod.Status.Phase != corev1.PodRunning {
		return ctrl.Result{}, podNotRunningErr
	}

	// Get the Containers of the Pod that are requesting exclusive CPUs.
	admissibleContainers := getAdmissibleContainers(pod, r.PodResourcesClient, &logger)
	if len(admissibleContainers) == 0 {
		logger.Info("no containers are requesting exclusive CPUs")
		return ctrl.Result{}, nil
	}
	podUID := pod.GetUID()
	logger.V(5).Info("retrieving the podUID", "UID", podUID)
	if podUID == "" {
		logger.Info("no pod UID found")
		return ctrl.Result{}, errors.NewServiceUnavailable("pod UID not found")
	}

	// Get the power containers requested by containers in the pod.
	powerContainers, recoveryErrs := r.getPowerProfileRequestsFromContainers(ctx, admissibleContainers, pod, &logger)
	logger.V(5).Info("retrieved power profiles and containers from pod requests")

	// dpdkContainerAssigned tracks whether a DPDK container in this pod has already
	// been assigned telemetry and scaling. Only one DPDK container per pod is supported.
	dpdkContainerAssigned := false
	// Reconcile CPU pools and track errors per container.
	// Skip containers that already have errors (e.g., profile unavailability).
	for i := range powerContainers {
		container := &powerContainers[i]
		// Skip CPU pool reconciliation for containers with existing errors.
		if len(container.Errors) > 0 {
			logger.V(5).Info("skipping CPU pool reconciliation for container with errors", "container", container.Name)
			continue
		}

		exclusivePool := r.PowerLibrary.GetExclusivePool(container.PowerProfile)
		if exclusivePool == nil {
			err := fmt.Errorf("exclusive pool for profile %s not found", container.PowerProfile)
			logger.Error(err, "failed to get exclusive pool", "container", container.Name)
			container.Errors = append(container.Errors, err.Error())
			recoveryErrs = append(recoveryErrs, err)
			continue
		}

		// Get actual CPUs currently in the exclusive pool.
		actualCPUs := exclusivePool.Cpus().IDs()

		// Compute delta: cores to add (in desired but not in actual).
		coresToAdd := detectCoresAdded(actualCPUs, container.CPUIDs, &logger)
		if len(coresToAdd) > 0 {
			// CPUs can only be moved to exclusive pool from shared pool.
			// If CPUs are still in the reserved pool, the shared workload hasn't been processed yet - requeue and wait for it.
			if !r.areCPUsInSharedPool(coresToAdd) {
				logger.Info("CPUs not yet in shared pool, waiting for shared workload to be processed", "cpus", coresToAdd)
				return ctrl.Result{RequeueAfter: queuetime}, nil
			}

			logger.V(5).Info("moving CPUs to exclusive pool", "profile", container.PowerProfile, "container", container.Name, "cpus", coresToAdd)
			if err := exclusivePool.MoveCPUIDs(coresToAdd); err != nil {
				logger.Error(err, "failed to move CPUs to exclusive pool", "profile", container.PowerProfile, "container", container.Name)
				container.Errors = append(container.Errors, err.Error())
				recoveryErrs = append(recoveryErrs, err)
				continue
			}
		}

		// Set up DPDK telemetry and scaling if the profile has a CPUScalingPolicy.
		if r.DPDKTelemetryClient == nil || r.CPUScalingManager == nil {
			continue
		}
		profile := &powerv1alpha1.PowerProfile{}
		if err := r.Get(ctx, client.ObjectKey{Namespace: PowerNamespace, Name: container.PowerProfile}, profile); err != nil {
			if errors.IsNotFound(err) {
				// Unlikely: profile was validated moments ago, but handle deletion between checks.
				errMsg := fmt.Sprintf("PowerProfile '%s' not found", container.PowerProfile)
				container.Errors = append(container.Errors, errMsg)
				recoveryErrs = append(recoveryErrs, errors.NewServiceUnavailable(errMsg))
				continue
			}
			return ctrl.Result{}, fmt.Errorf("failed to get PowerProfile: %w", err)
		}
		if profile.Spec.CPUScalingPolicy != nil && profile.Spec.CPUScalingPolicy.WorkloadType == WorkloadTypePollingDPDK {
			if dpdkContainerAssigned {
				container.Errors = append(container.Errors,
					"DPDK dynamic frequency scaling is only supported for a single container per pod; this container is skipped")
			} else {
				dpdkContainerAssigned = true
				// Ensure a DPDK telemetry connection exists for this pod.
				r.DPDKTelemetryClient.EnsureConnection(&scaling.DPDKTelemetryConnectionData{
					PodUID:      string(podUID),
					WatchedCPUs: container.CPUIDs,
				})
				// Build per-CPU scaling options.
				scalingOpts, err := r.generateCPUScalingOpts(profile.Spec.CPUScalingPolicy, container.CPUIDs)
				if err != nil {
					msg := "some CPUs could not be configured for DPDK scaling"
					logger.Error(err, msg, "container", container.Name)
					container.Errors = append(container.Errors, fmt.Sprintf("%s: %v", msg, err))
				}
				if len(scalingOpts) > 0 {
					r.CPUScalingManager.AddCPUScaling(scalingOpts)
				}
			}
		}
	}

	// Update PowerNodeState status with container info (errors are already on each PowerContainer).
	if err := r.addPowerNodeStatusExclusiveEntry(ctx, nodeName, string(podUID), pod.Name, powerContainers, &logger); err != nil {
		return ctrl.Result{}, err
	}

	// Finally, update the controller's state
	logger.V(5).Info("updating the controller's internal state")
	guaranteedPod := powerv1alpha1.GuaranteedPod{}
	guaranteedPod.Node = pod.Spec.NodeName
	guaranteedPod.Name = pod.GetName()
	guaranteedPod.Namespace = pod.Namespace
	guaranteedPod.UID = string(podUID)
	stateContainers := make([]powerv1alpha1.Container, 0, len(powerContainers))
	for _, pc := range powerContainers {
		stateContainers = append(stateContainers, powerv1alpha1.Container{
			Name:          pc.Name,
			ID:            pc.ID,
			PowerProfile:  pc.PowerProfile,
			ExclusiveCPUs: pc.CPUIDs,
		})
	}
	guaranteedPod.Containers = stateContainers
	err = r.State.UpdateStateGuaranteedPods(guaranteedPod)
	if err != nil {
		logger.Error(err, "error updating the internal state")
		return ctrl.Result{}, err
	}

	wrappedErrs := e.Join(recoveryErrs...)
	if wrappedErrs != nil {
		logger.Error(wrappedErrs, "recoverable errors")
		return ctrl.Result{Requeue: false}, fmt.Errorf("recoverable errors encountered: %w", wrappedErrs)
	}
	return ctrl.Result{}, nil
}

// handlePodDeletion cleans up when a guaranteed pod is deleted or succeeded. It moves the pod's
// exclusive CPUs back to the shared pool, tears down any DPDK telemetry/scaling, and removes
// the pod's entry from PowerNodeState.
func (r *PowerPodReconciler) handlePodDeletion(ctx context.Context, pod *corev1.Pod, nodeName string, logger *logr.Logger) (ctrl.Result, error) {
	// Delete the Pod from the internal state in case it was never deleted
	_ = r.State.DeletePodFromState(pod.GetName(), pod.GetNamespace())

	// Get the pod's CPU assignments from PowerNodeState to know what to clean up.
	powerNodeStateName := fmt.Sprintf("%s-power-state", nodeName)
	currentNodeState := &powerv1alpha1.PowerNodeState{}

	// Get the PowerNodeState to find this pod's CPU assignments and move them back to the shared pool.
	err := r.Get(ctx, client.ObjectKey{Namespace: PowerNamespace, Name: powerNodeStateName}, currentNodeState)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.Info("WARNING: PowerNodeState not found during pod deletion, CPUs may remain in exclusive pool", "powerNodeState", powerNodeStateName)
		} else {
			logger.Error(err, "failed to get PowerNodeState")
			return ctrl.Result{}, err
		}
	} else if currentNodeState.Status.CPUPools != nil {
		// Move the pod's CPUs back to the shared pool.
		var deletedCPUIDs []uint
		for _, exclusive := range currentNodeState.Status.CPUPools.Exclusive {
			if exclusive.PodUID == string(pod.GetUID()) {
				for _, container := range exclusive.PowerContainers {
					logger.V(5).Info("moving CPUs back to shared pool", "container", container.Name, "profile", container.PowerProfile, "cpus", container.CPUIDs)
					if err := r.PowerLibrary.GetSharedPool().MoveCPUIDs(container.CPUIDs); err != nil {
						logger.Error(err, "failed to move CPUs back to shared pool", "container", container.Name, "profile", container.PowerProfile)
						return ctrl.Result{}, err
					}
					deletedCPUIDs = append(deletedCPUIDs, container.CPUIDs...)
				}
				break
			}
		}

		// Tear down DPDK telemetry and scaling for this pod's CPUs.
		// No-op for non-DPDK pods: CloseConnection and RemoveCPUScaling
		// safely ignore entries that don't exist.
		if r.DPDKTelemetryClient != nil && r.CPUScalingManager != nil {
			r.DPDKTelemetryClient.CloseConnection(string(pod.GetUID()))
			r.CPUScalingManager.RemoveCPUScaling(deletedCPUIDs)
		}
	}

	// Remove the pod's entry from PowerNodeState via SSA.
	if err := r.removePowerNodeStatusExclusiveEntry(ctx, nodeName, string(pod.GetUID()), logger); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *PowerPodReconciler) getPowerProfileRequestsFromContainers(ctx context.Context, containers []corev1.Container, pod *corev1.Pod, logger *logr.Logger) ([]powerv1alpha1.PowerContainer, []error) {
	logger.V(5).Info("get the power profiles from the containers")
	var recoverableErrs []error
	powerContainers := make([]powerv1alpha1.PowerContainer, 0)
	for _, container := range containers {
		logger.V(5).Info("retrieving the requested power profile from the container spec")
		containerID := strings.TrimPrefix(getContainerID(pod, container.Name), "docker://")

		profileName, requestNum, err := getContainerProfileFromRequests(container, logger)
		if err != nil {
			// Pod spec validation errors are not recoverable (pod spec is immutable).
			// Store the error in PowerNodeState for visibility but don't trigger requeue.
			logger.Error(err, "pod spec validation error", "container", container.Name)
			powerContainers = append(powerContainers, powerv1alpha1.PowerContainer{
				Name:   container.Name,
				ID:     containerID,
				CPUIDs: []uint{},
				Errors: []string{err.Error()},
			})
			continue
		}

		// If there was no profile requested in this container we can move onto the next one
		if profileName == "" {
			logger.V(5).Info("no profile was requested by the container")
			continue
		}
		profileAvailable, err := validateProfileAvailabilityOnNode(ctx, r.Client, profileName, pod.Spec.NodeName, r.PowerLibrary, logger)
		if err != nil {
			logger.Error(err, "error checking if power profile is available on node")
			continue
		}
		if !profileAvailable {
			errMsg := fmt.Sprintf("power profile '%s' is not available on node %s", profileName, pod.Spec.NodeName)
			recoverableErrs = append(recoverableErrs, errors.NewServiceUnavailable(errMsg))

			// Add the container with its error so it appears in PowerNodeState.
			// CPU pool reconciliation will be skipped for containers with errors.
			powerContainers = append(powerContainers, powerv1alpha1.PowerContainer{
				Name:         container.Name,
				ID:           containerID,
				PowerProfile: profileName,
				CPUIDs:       []uint{},
				Errors:       []string{errMsg},
			})
			continue
		}
		coreIDs, err := r.PodResourcesClient.GetContainerCPUs(pod.GetName(), container.Name)
		if err != nil {
			logger.V(5).Info("error getting CoreIDs.", "ContainerID", containerID)
			recoverableErrs = append(recoverableErrs, err)
			continue
		}
		cleanCoreList := getCleanCoreList(coreIDs)
		logger.V(5).Info("reserving cores to container.", "ContainerID", containerID, "Cores", cleanCoreList)

		// Accounts for case where cores acquired through DRA don't match profile requests.
		if len(cleanCoreList) != requestNum {
			recoverableErrs = append(recoverableErrs, fmt.Errorf("assigned cores did not match requested profiles. cores:%d, profiles %d", len(cleanCoreList), requestNum))
			continue
		}
		logger.V(5).Info("creating the power container.", "ContainerID", containerID, "Cores", cleanCoreList, "Profile", profileName)
		powerContainers = append(powerContainers, powerv1alpha1.PowerContainer{
			Name:         container.Name,
			ID:           containerID,
			CPUIDs:       cleanCoreList,
			PowerProfile: profileName,
		})
	}

	return powerContainers, recoverableErrs
}

func getContainerProfileFromRequests(container corev1.Container, logger *logr.Logger) (string, int, error) {
	profileName := ""
	moreThanOneProfileError := errors.NewServiceUnavailable("cannot have more than one power profile per container")
	resourceRequestsMismatchError := errors.NewServiceUnavailable("mismatch between CPU requests and the power profile requests")
	for resource := range container.Resources.Requests {
		if strings.HasPrefix(string(resource), ResourcePrefix) {
			if profileName == "" {
				profileName = string(resource[len(ResourcePrefix):])
			} else {
				// Cannot have more than one profile for a singular container
				return "", 0, moreThanOneProfileError
			}
		}
	}
	var intProfileRequests int
	if profileName != "" {
		// Check if there is a mismatch in CPU requests and the power profile requests
		logger.V(5).Info("confirming that CPU requests and the power profiles request match")
		powerProfileResourceName := corev1.ResourceName(fmt.Sprintf("%s%s", ResourcePrefix, profileName))
		numRequestsPowerProfile := container.Resources.Requests[powerProfileResourceName]
		numLimitsPowerProfile := container.Resources.Limits[powerProfileResourceName]
		intProfileRequests = int(numRequestsPowerProfile.Value())
		intProfileLimits := int(numLimitsPowerProfile.Value())

		numRequestsCPU, numLimitsCPU := checkResource(container, CPUResource, 0, 0)

		// if previous checks fail we need to account for resource claims
		// if there's a problem with core numbers we'll catch it
		// before moving cores to pools by comparing intProfileRequests with assigned cores
		if numRequestsCPU == 0 && len(container.Resources.Claims) > 0 {
			return profileName, intProfileRequests, nil
		}
		if numRequestsCPU != intProfileRequests ||
			numLimitsCPU != intProfileLimits {
			return "", 0, resourceRequestsMismatchError
		}
	}

	return profileName, intProfileRequests, nil
}

func checkResource(container corev1.Container, resource corev1.ResourceName, numRequestsCPU int, numLimitsCPU int) (int, int) {
	numRequestsDevice := container.Resources.Requests[resource]
	numRequestsCPU += int(numRequestsDevice.Value())

	numLimitsDevice := container.Resources.Limits[resource]
	numLimitsCPU += int(numLimitsDevice.Value())
	return numRequestsCPU, numLimitsCPU
}

func allPodContainers(pod *corev1.Pod) []corev1.Container {
	containers := make([]corev1.Container, 0, len(pod.Spec.InitContainers)+len(pod.Spec.Containers))
	containers = append(containers, pod.Spec.InitContainers...)
	containers = append(containers, pod.Spec.Containers...)
	return containers
}

func getAdmissibleContainers(pod *corev1.Pod, resourceClient podresourcesclient.PodResourcesClient, logger *logr.Logger) []corev1.Container {

	logger.V(5).Info("receiving containers requesting exclusive CPUs")
	admissibleContainers := make([]corev1.Container, 0)
	containerList := allPodContainers(pod)
	controlPlaneAvailable := pingControlPlane(resourceClient)
	for _, container := range containerList {
		if doesContainerRequireExclusiveCPUs(pod, &container, logger) || controlPlaneAvailable {
			admissibleContainers = append(admissibleContainers, container)
		}
	}
	logger.V(5).Info("containers requesting exclusive resources are: ", "Containers", admissibleContainers)
	return admissibleContainers
}

func detectCoresAdded(originalCoreList []uint, updatedCoreList []uint, logger *logr.Logger) []uint {
	var coresAdded []uint
	logger.V(5).Info("detecting if cores are added to the cores list")
	for _, core := range updatedCoreList {
		if !slices.Contains(originalCoreList, core) {
			coresAdded = append(coresAdded, core)
		}
	}

	return coresAdded
}

func doesContainerRequireExclusiveCPUs(pod *corev1.Pod, container *corev1.Container, logger *logr.Logger) bool {
	if pod.Status.QOSClass != corev1.PodQOSGuaranteed {
		logger.V(3).Info(fmt.Sprintf("pod %s is not in guaranteed quality of service class", pod.Name))
		return false
	}

	cpuQuantity := container.Resources.Requests[corev1.ResourceCPU]
	return cpuQuantity.Value()*1000 == cpuQuantity.MilliValue()
}

func pingControlPlane(client podresourcesclient.PodResourcesClient) bool {
	// see if the socket sends a response
	req := podresourcesapi.ListPodResourcesRequest{}
	_, err := client.CPUControlPlaneClient.List(context.TODO(), &req)
	return err == nil
}

func getContainerID(pod *corev1.Pod, containerName string) string {
	statuses := make([]corev1.ContainerStatus, 0, len(pod.Status.InitContainerStatuses)+len(pod.Status.ContainerStatuses))
	statuses = append(statuses, pod.Status.InitContainerStatuses...)
	statuses = append(statuses, pod.Status.ContainerStatuses...)
	for _, containerStatus := range statuses {
		if containerStatus.Name == containerName {
			return containerStatus.ContainerID
		}
	}

	return ""
}

func getCleanCoreList(coreIDs string) []uint {
	cleanCores := make([]uint, 0)
	commaSeparated := strings.Split(coreIDs, ",")
	for _, splitCore := range commaSeparated {
		hyphenSeparated := strings.Split(splitCore, "-")
		if len(hyphenSeparated) == 1 {
			intCore, err := strconv.ParseUint(hyphenSeparated[0], 10, 32)
			if err != nil {
				fmt.Printf("error getting the core list: %v", err)
				return []uint{}
			}
			cleanCores = append(cleanCores, uint(intCore))
		} else {
			startCore, err := strconv.Atoi(hyphenSeparated[0])
			if err != nil {
				fmt.Printf("error getting the core list: %v", err)
				return []uint{}
			}
			endCore, err := strconv.Atoi(hyphenSeparated[len(hyphenSeparated)-1])
			if err != nil {
				fmt.Printf("error getting the core list: %v", err)
				return []uint{}
			}
			for i := startCore; i <= endCore; i++ {
				cleanCores = append(cleanCores, uint(i))
			}
		}
	}

	return cleanCores
}

// PowerReleventPodPredicate returns true if this pod should be considered by the controller
// based on node scope and presence of power profile resource requests.
func PowerReleventPodPredicate(obj client.Object) bool {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return false
	}
	if pod.Spec.NodeName != os.Getenv("NODE_NAME") {
		return false
	}
	for _, c := range allPodContainers(pod) {
		// No need to check the limits, as if the requests are present,
		// the limits must be present as well.
		for rn := range c.Resources.Requests {
			if strings.HasPrefix(string(rn), ResourcePrefix) {
				return true
			}
		}
	}
	return false
}

// applyPowerNodeStateExclusiveStatus applies the given exclusive CPU pool entries to the
// PowerNodeState status using Server-Side Apply. The fieldManager parameter enables per-pod
// ownership — each pod should use a unique field manager (e.g., "powerpod-controller.pod-uid")
// so that SSA can track ownership at the element level for the map-type Exclusive list.
func (r *PowerPodReconciler) applyPowerNodeStateExclusiveStatus(ctx context.Context, powerNodeStateName string, exclusive []powerv1alpha1.ExclusiveCPUPoolStatus, fieldManager string) error {
	patchNodeState := &powerv1alpha1.PowerNodeState{
		TypeMeta: metav1.TypeMeta{
			APIVersion: powerv1alpha1.GroupVersion.String(),
			Kind:       PowerNodeStateKind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      powerNodeStateName,
			Namespace: PowerNamespace,
		},
		Status: powerv1alpha1.PowerNodeStateStatus{
			CPUPools: &powerv1alpha1.CPUPoolsStatus{
				Exclusive: exclusive,
			},
		},
	}

	return r.Status().Patch(ctx, patchNodeState, client.Apply, client.FieldOwner(fieldManager), client.ForceOwnership)
}

// addPowerNodeStatusExclusiveEntry updates the PowerNodeState status with exclusive CPU pool
// information for this pod's containers, including any errors encountered.
func (r *PowerPodReconciler) addPowerNodeStatusExclusiveEntry(
	ctx context.Context,
	nodeName string,
	podUID string,
	podName string,
	powerContainers []powerv1alpha1.PowerContainer,
	logger *logr.Logger,
) error {
	powerNodeStateName := fmt.Sprintf("%s-power-state", nodeName)
	fieldManager := fmt.Sprintf("powerpod-controller.%s", podUID)

	entry := []powerv1alpha1.ExclusiveCPUPoolStatus{
		{
			PodUID:          podUID,
			Pod:             podName,
			PowerContainers: powerContainers,
		},
	}

	logger.V(5).Info("updating PowerNodeState status with SSA", "fieldManager", fieldManager)
	if err := r.applyPowerNodeStateExclusiveStatus(ctx, powerNodeStateName, entry, fieldManager); err != nil {
		if errors.IsNotFound(err) {
			// CPUs were already moved to exclusive in POL but status wasn't recorded.
			// Return error to requeue so we retry once PowerConfig re-creates the CR.
			logger.Info("PowerNodeState not found, requeueing to record exclusive pool status", "powerNodeState", powerNodeStateName)
			return err
		}
		logger.Error(err, "failed to update PowerNodeState status")
		return err
	}

	return nil
}

// removePowerNodeStatusExclusiveEntry removes a pod's exclusive entry from the PowerNodeState status.
//
// Applying an empty Exclusive list causes SSA to prune the entry this manager previously
// owned, while preserving entries owned by other field managers.
func (r *PowerPodReconciler) removePowerNodeStatusExclusiveEntry(
	ctx context.Context,
	nodeName string,
	podUID string,
	logger *logr.Logger,
) error {
	powerNodeStateName := fmt.Sprintf("%s-power-state", nodeName)
	fieldManager := fmt.Sprintf("powerpod-controller.%s", podUID)

	logger.V(5).Info("removing exclusive entry from PowerNodeState via SSA", "fieldManager", fieldManager)
	if err := r.applyPowerNodeStateExclusiveStatus(ctx, powerNodeStateName, []powerv1alpha1.ExclusiveCPUPoolStatus{}, fieldManager); err != nil {
		if errors.IsNotFound(err) {
			logger.V(5).Info("PowerNodeState not found, skipping exclusive pool status cleanup", "powerNodeState", powerNodeStateName)
			return nil
		}
		logger.Error(err, "failed to remove exclusive entry from PowerNodeState status")
		return err
	}

	return nil
}

// areCPUsInSharedPool checks if all specified CPUs are currently in the shared pool.
// Returns false if any CPU is still in the reserved pool (shared workload not yet processed).
func (r *PowerPodReconciler) areCPUsInSharedPool(cpuIDs []uint) bool {
	sharedCPUIDs := r.PowerLibrary.GetSharedPool().Cpus().IDs()
	for _, cpuID := range cpuIDs {
		if !slices.Contains(sharedCPUIDs, cpuID) {
			return false
		}
	}
	return true
}

// generateCPUScalingOpts translates a CPUScalingPolicy and a set of CPUs
// into a list of per-CPU scaling options used by the CPUScalingManager.
func (r *PowerPodReconciler) generateCPUScalingOpts(scalingPolicy *powerv1alpha1.CPUScalingPolicy, cpuIDs []uint) ([]scaling.CPUScalingOpts, error) {
	allCpus := r.PowerLibrary.GetAllCpus()
	optsList := make([]scaling.CPUScalingOpts, 0, len(cpuIDs))

	var missingCPUs []uint
	for _, id := range cpuIDs {
		cpu := allCpus.ByID(id)
		if cpu == nil {
			missingCPUs = append(missingCPUs, id)
			continue
		}

		// Get the min and max frequency of this CPU and calculate the fallback
		// and application frequencies.
		minFreq, maxFreq := cpu.GetAbsMinMax()
		fallbackFreqPct := uint(*scalingPolicy.FallbackFreqPercent)
		fallbackFreq := minFreq + (maxFreq-minFreq)*(fallbackFreqPct)/100
		appFreqPct := uint(*scalingPolicy.AppFreqPercent)
		appFreq := minFreq + (maxFreq-minFreq)*(appFreqPct)/100

		opts := scaling.CPUScalingOpts{
			CPU:                        cpu,
			SamplePeriod:               scalingPolicy.SamplePeriod.Duration,
			CooldownPeriod:             scalingPolicy.CooldownPeriod.Duration,
			TargetUsage:                *scalingPolicy.TargetUsage,
			AllowedUsageDifference:     *scalingPolicy.AllowedUsageDifference,
			AllowedFrequencyDifference: *scalingPolicy.AllowedFrequencyDifference * 1000,
			HWMaxFrequency:             int(maxFreq),
			HWMinFrequency:             int(minFreq),
			CurrentTargetFrequency:     scaling.FrequencyNotYetSet,
			ScaleFactor:                float64(*scalingPolicy.ScalePercentage) / 100.0,
			FallbackFreq:               int(fallbackFreq),
			AppFreq:                    int(appFreq),
			SiblingCPUIDs:              cpu.GetSiblings(),
		}
		optsList = append(optsList, opts)
	}

	var err error
	if len(missingCPUs) > 0 {
		err = fmt.Errorf("CPUs %v not found in power library", missingCPUs)
	}
	return optsList, err
}

func (r *PowerPodReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Register a field index on Pod.spec.nodeName so that profileToPodRequests
	// can efficiently list pods on this node when a profile's CPUScalingPolicy changes.
	if err := mgr.GetFieldIndexer().IndexField(context.TODO(), &corev1.Pod{}, "spec.nodeName",
		func(obj client.Object) []string {
			return []string{obj.(*corev1.Pod).Spec.NodeName}
		}); err != nil {
		return fmt.Errorf("failed to create pod node name field index: %w", err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{},
			builder.WithPredicates(
				predicate.NewPredicateFuncs(PowerReleventPodPredicate))).
		Watches(&powerv1alpha1.PowerProfile{},
			handler.EnqueueRequestsFromMapFunc(r.powerProfileToPodRequests),
			builder.WithPredicates(predicate.Funcs{
				UpdateFunc: func(e event.UpdateEvent) bool {
					oldProfile := e.ObjectOld.(*powerv1alpha1.PowerProfile)
					newProfile := e.ObjectNew.(*powerv1alpha1.PowerProfile)

					if newProfile.Spec.Shared {
						return false
					}
					// Filter for CPU scaling policy changes only.
					return !reflect.DeepEqual(oldProfile.Spec.CPUScalingPolicy, newProfile.Spec.CPUScalingPolicy)
				},
				CreateFunc:  func(e event.CreateEvent) bool { return false },
				GenericFunc: func(ge event.GenericEvent) bool { return false },
				DeleteFunc:  func(de event.DeleteEvent) bool { return false },
			})).
		Complete(r)
}

// powerProfileToPodRequests returns reconcile requests for all pods on this node
// that use the given PowerProfile. This re-reconciles their DPDK scaling
// when a profile's CPUScalingPolicy changes.
func (r *PowerPodReconciler) powerProfileToPodRequests(ctx context.Context, obj client.Object) []reconcile.Request {
	requests := []reconcile.Request{}

	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		return requests
	}

	podList := &corev1.PodList{}
	if err := r.Client.List(ctx, podList, client.MatchingFields{"spec.nodeName": nodeName}); err != nil {
		r.Log.Error(err, "failed to list pods for profile change")
		return requests
	}

	profileName := obj.GetName()
	for _, pod := range podList.Items {
		for _, c := range allPodContainers(&pod) {
			for rn := range c.Resources.Requests {
				if string(rn) == ResourcePrefix+profileName {
					requests = append(requests, reconcile.Request{
						NamespacedName: types.NamespacedName{
							Name:      pod.Name,
							Namespace: pod.Namespace,
						},
					})
				}
			}
		}
	}
	return requests
}
