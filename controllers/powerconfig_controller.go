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
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	powerv1alpha1 "github.com/cluster-power-manager/cluster-power-manager/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/cluster-power-manager/cluster-power-manager/pkg/state"
)

const (
	ExtendedResourcePrefix   = "power.cluster-power-manager.github.io/"
	NodeAgentDSName          = "power-node-agent"
	relatedImageNodeAgentEnv = "RELATED_IMAGE_NODE_AGENT"

	// FieldOwnerPowerConfigController is the SSA field manager for NodeInfo in PowerNodeState.
	FieldOwnerPowerConfigController = "powerconfig-controller"
)

var NodeAgentDaemonSetPath = "/power-manifests/power-node-agent-ds.yaml"

// PowerConfigReconciler reconciles a PowerConfig object
type PowerConfigReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
	State  *state.PowerNodeData
}

// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch,roleName=manager-role
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=create;delete;update;patch,roleName=manager-role
// +kubebuilder:rbac:groups=power.cluster-power-manager.github.io,resources=powerconfigs,verbs=get;list;watch;delete,roleName=manager-role
// +kubebuilder:rbac:groups=power.cluster-power-manager.github.io,resources=powerconfigs/status,verbs=get;patch,roleName=manager-role
// +kubebuilder:rbac:groups=power.cluster-power-manager.github.io,resources=powerprofiles,verbs=get;list;watch;delete,roleName=manager-role
// +kubebuilder:rbac:groups=power.cluster-power-manager.github.io,resources=powernodestates,verbs=get;list;watch;create;delete,roleName=manager-role
// +kubebuilder:rbac:groups=power.cluster-power-manager.github.io,resources=powernodestates/status,verbs=get;patch,roleName=manager-role

func (r *PowerConfigReconciler) Reconcile(c context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = context.Background()
	logger := r.Log.WithValues("powerconfig", req.NamespacedName)

	if req.Namespace != PowerNamespace {
		err := fmt.Errorf("incorrect namespace")
		logger.Error(err, "resource is not in the power-manager namespace, ignoring")
		return ctrl.Result{}, err
	}

	configs := &powerv1alpha1.PowerConfigList{}
	logger.V(5).Info("retrieving the power config list")
	err := r.Client.List(c, configs)
	if err != nil {
		logger.Error(err, "error retrieving the power config list")
		return ctrl.Result{}, err
	}

	config := &powerv1alpha1.PowerConfig{}
	logger.V(5).Info("retrieving the power config")
	err = r.Client.Get(c, req.NamespacedName, config)
	if err != nil {
		logger.V(5).Info("failed retrieving the power config, checking if exists")
		if errors.IsNotFound(err) {
			// Power config was deleted, if the number of power configs is > 0, don't delete the power profiles
			if len(configs.Items) == 0 {
				powerProfiles := &powerv1alpha1.PowerProfileList{}
				err = r.Client.List(c, powerProfiles)
				logger.V(5).Info("retrieving all power profiles in the cluster")
				if err != nil {
					logger.Error(err, "error retrieving the power profiles")
					return ctrl.Result{}, err
				}

				for _, profile := range powerProfiles.Items {
					err = r.Client.Delete(c, &profile)
					logger.V(5).Info(fmt.Sprintf("deleting power profile %s", profile.Name))
					if err != nil {
						logger.Error(err, fmt.Sprintf("error deleting power profile '%s' from cluster", profile.Name))
						return ctrl.Result{}, err
					}
				}

				// Delete all the PowerNodeStates CRs.
				powerNodeStates := &powerv1alpha1.PowerNodeStateList{}
				err = r.Client.List(c, powerNodeStates)
				logger.V(5).Info("retrieving all PowerNodeStates in the cluster")
				if err != nil {
					logger.Error(err, "error retrieving PowerNodeStates")
					return ctrl.Result{}, err
				}

				for _, nodeState := range powerNodeStates.Items {
					logger.V(5).Info(fmt.Sprintf("deleting PowerNodeState %s", nodeState.Name))
					err = r.Client.Delete(c, &nodeState)
					if err != nil {
						logger.Error(err, fmt.Sprintf("error deleting PowerNodeState '%s' from cluster", nodeState.Name))
						return ctrl.Result{}, err
					}
				}

				daemonSet := &appsv1.DaemonSet{}
				logger.V(5).Info("retrieving the power node-agent daemonSet")
				err = r.Client.Get(c, client.ObjectKey{
					Name:      NodeAgentDSName,
					Namespace: PowerNamespace,
				}, daemonSet)
				if err != nil {
					if !errors.IsNotFound(err) {
						logger.Error(err, "error retrieving the power node-agent daemonSet")
						return ctrl.Result{}, err
					}
				} else {
					err = r.Client.Delete(c, daemonSet)
					if err != nil {
						logger.Error(err, "error deleting the power node-agent daemonset")
						return ctrl.Result{}, err
					}
				}
			}

			return ctrl.Result{}, nil
		}

		logger.Error(err, "error retrieving the power config")
		return ctrl.Result{}, err
	}

	if len(configs.Items) > 1 {
		logger.V(5).Info("checking to make sure there is only one power config")
		moreThanOneConfigError := errors.NewServiceUnavailable("cannot have more than one power config")
		logger.Error(moreThanOneConfigError, "error reconciling the power config")

		err = r.Client.Delete(c, config)
		if err != nil {
			logger.Error(err, "error deleting the power config")
			return ctrl.Result{}, err
		}

		return ctrl.Result{}, nil
	}

	// Create power node-agent daemonSet
	logger.V(5).Info("creating the power node-agent daemonSet")
	err = r.createDaemonSetIfNotPresent(c, config, NodeAgentDaemonSetPath, &logger)
	if err != nil {
		logger.Error(err, "error creating the power node-agent")
		return ctrl.Result{}, err
	}

	labelledNodeList := &corev1.NodeList{}
	listOption := config.Spec.PowerNodeSelector

	logger.V(5).Info("confirming desired nodes match the power node selector")
	err = r.Client.List(c, labelledNodeList, client.MatchingLabels(listOption))
	if err != nil {
		logger.Info("failed to list nodes with power node selector", listOption)
		return ctrl.Result{}, err
	}

	for _, node := range labelledNodeList.Items {
		logger.V(5).Info("updating the node name")
		r.State.UpdatePowerNodeData(node.Name)

		// Create PowerNodeState for this node if it doesn't exist
		powerNodeState := &powerv1alpha1.PowerNodeState{}
		powerNodeStateName := fmt.Sprintf("%s-power-state", node.Name)
		err = r.Client.Get(c, client.ObjectKey{
			Namespace: PowerNamespace,
			Name:      powerNodeStateName,
		}, powerNodeState)

		if err != nil {
			if errors.IsNotFound(err) {
				logger.V(5).Info(fmt.Sprintf("creating the PowerNodeState CR %s", powerNodeStateName))
				powerNodeState = &powerv1alpha1.PowerNodeState{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: PowerNamespace,
						Name:      powerNodeStateName,
					},
				}

				err = r.Client.Create(c, powerNodeState)
				if err != nil {
					logger.Error(err, "error creating the PowerNodeState CR")
					return ctrl.Result{}, err
				}

				// Write NodeInfo via SSA once at creation time.
				if err := r.applyNodeInfo(c, &node, powerNodeStateName, &logger); err != nil {
					return ctrl.Result{}, err
				}
			} else {
				return ctrl.Result{}, err
			}
		}
	}

	patch := client.MergeFrom(config.DeepCopy())
	config.Status.Nodes = r.State.PowerNodeList
	err = r.Client.Status().Patch(c, config, patch)
	if err != nil {
		logger.Error(err, "failed to update the power config")
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: time.Second * 5}, nil
}

// applyNodeInfo writes static node information to PowerNodeState status via SSA.
func (r *PowerConfigReconciler) applyNodeInfo(ctx context.Context, node *corev1.Node, powerNodeStateName string, logger *logr.Logger) error {
	cpuCapacity := int(node.Status.Capacity.Cpu().Value())
	arch := node.Labels[corev1.LabelArchStable]

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
			NodeInfo: &powerv1alpha1.NodeInfo{
				CPUCapacity:  cpuCapacity,
				Architecture: arch,
			},
		},
	}

	if err := r.Status().Patch(ctx, patchNodeState, client.Apply,
		client.FieldOwner(FieldOwnerPowerConfigController), client.ForceOwnership); err != nil {
		logger.Error(err, "failed to apply NodeInfo to PowerNodeState", "powerNodeState", powerNodeStateName)
		return err
	}

	logger.V(5).Info("applied NodeInfo to PowerNodeState",
		"powerNodeState", powerNodeStateName,
		"cpuCapacity", cpuCapacity,
		"architecture", arch)
	return nil
}

func (r *PowerConfigReconciler) createDaemonSetIfNotPresent(c context.Context, powerConfig *powerv1alpha1.PowerConfig, path string, logger *logr.Logger) error {
	logger.V(5).Info("creating the daemonSet")

	daemonSet := &appsv1.DaemonSet{}
	var err error

	err = r.Client.Get(c, client.ObjectKey{
		Name:      NodeAgentDSName,
		Namespace: PowerNamespace,
	}, daemonSet)
	if err != nil {
		if errors.IsNotFound(err) {
			daemonSet, err = createDaemonSetFromManifest(path)
			if err != nil {
				logger.Error(err, "error creating the daemonSet")
				return err
			}
			if _, err := applyNodeAgentImage(daemonSet, os.Getenv(relatedImageNodeAgentEnv)); err != nil {
				return err
			}
			if len(powerConfig.Spec.PowerNodeSelector) != 0 {
				daemonSet.Spec.Template.Spec.NodeSelector = powerConfig.Spec.PowerNodeSelector
			}
			err = r.Client.Create(c, daemonSet)
			if err != nil {
				logger.Error(err, "error creating the daemonSet")
				return err
			}
			logger.V(5).Info("new power node-agent daemonSet created")
			return nil
		}
		return err
	}

	// Update the existing DaemonSet when its image or node selector changes.
	changed, err := applyNodeAgentImage(daemonSet, os.Getenv(relatedImageNodeAgentEnv))
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(daemonSet.Spec.Template.Spec.NodeSelector, powerConfig.Spec.PowerNodeSelector) {
		daemonSet.Spec.Template.Spec.NodeSelector = powerConfig.Spec.PowerNodeSelector
		changed = true
	}
	if changed {
		logger.V(5).Info("updating the existing daemonSet")
		err = r.Client.Update(c, daemonSet)
		if err != nil {
			logger.Error(err, "error updating the power node-agent daemonSet")
			return err
		}
	}

	return nil
}

func findNodeAgentContainer(containers []corev1.Container) *corev1.Container {
	for i := range containers {
		if containers[i].Name == NodeAgentDSName {
			return &containers[i]
		}
	}
	return nil
}

func applyNodeAgentImage(ds *appsv1.DaemonSet, image string) (imageChanged bool, err error) {
	if image == "" {
		return false, nil
	}

	container := findNodeAgentContainer(ds.Spec.Template.Spec.Containers)
	if container == nil {
		return false, fmt.Errorf("container %q not found in node-agent DaemonSet", NodeAgentDSName)
	}
	if container.Image == image {
		return false, nil
	}
	container.Image = image
	return true, nil
}

func createDaemonSetFromManifest(path string) (*appsv1.DaemonSet, error) {
	yamlFile, err := os.ReadFile(path) //nolint:gosec // path is from NodeAgentDaemonSetPath, not user input
	if err != nil {
		return nil, err
	}

	decode := scheme.Codecs.UniversalDeserializer().Decode
	obj, _, err := decode(yamlFile, nil, nil)
	if err != nil {
		return nil, err
	}

	nodeAgentDaemonSet := obj.(*appsv1.DaemonSet)
	return nodeAgentDaemonSet, nil
}

func (r *PowerConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&powerv1alpha1.PowerConfig{}).
		Complete(r)
}
