package controllers

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"go.uber.org/zap/zapcore"
	grpc "google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	powerv1alpha1 "github.com/cluster-power-manager/cluster-power-manager/api/v1alpha1"
	"github.com/cluster-power-manager/cluster-power-manager/internal/power"
	"github.com/cluster-power-manager/cluster-power-manager/internal/scaling"
	"github.com/cluster-power-manager/cluster-power-manager/pkg/podresourcesclient"
	"github.com/cluster-power-manager/cluster-power-manager/pkg/podstate"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Your mock client probably has a status writer struct like this
type errSubResourceClient struct {
	*errClient
}

func (e *errSubResourceClient) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	return fmt.Errorf("mock client Create error")
}

func (e *errSubResourceClient) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	return fmt.Errorf("mock client Update error")
}

func (e *errSubResourceClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	return fmt.Errorf("mock client Patch error")
}

type fakePodResourcesClient struct {
	listResponse *podresourcesapi.ListPodResourcesResponse
}

func (f *fakePodResourcesClient) List(ctx context.Context, in *podresourcesapi.ListPodResourcesRequest, opts ...grpc.CallOption) (*podresourcesapi.ListPodResourcesResponse, error) {
	return f.listResponse, nil
}

func (f *fakePodResourcesClient) GetAllocatableResources(ctx context.Context, in *podresourcesapi.AllocatableResourcesRequest, opts ...grpc.CallOption) (*podresourcesapi.AllocatableResourcesResponse, error) {
	return &podresourcesapi.AllocatableResourcesResponse{}, nil
}

func (f *fakePodResourcesClient) Get(ctx context.Context, in *podresourcesapi.GetPodResourcesRequest, opts ...grpc.CallOption) (*podresourcesapi.GetPodResourcesResponse, error) {
	return &podresourcesapi.GetPodResourcesResponse{}, nil
}

func createFakePodResourcesListerClient(fakePodResources []*podresourcesapi.PodResources) *podresourcesclient.PodResourcesClient {
	fakeListResponse := &podresourcesapi.ListPodResourcesResponse{
		PodResources: fakePodResources,
	}

	podResourcesListerClient := &fakePodResourcesClient{}
	podResourcesListerClient.listResponse = fakeListResponse
	return &podresourcesclient.PodResourcesClient{Client: podResourcesListerClient, CPUControlPlaneClient: podResourcesListerClient}
}

func createPodReconcilerObject(objs []runtime.Object, podResourcesClient *podresourcesclient.PodResourcesClient) (*PowerPodReconciler, error) {
	log.SetLogger(zap.New(
		zap.UseDevMode(true),
		func(opts *zap.Options) {
			opts.TimeEncoder = zapcore.ISO8601TimeEncoder
		},
	),
	)
	// register operator types with the runtime scheme.
	s := scheme.Scheme

	// add route Openshift scheme
	if err := powerv1alpha1.AddToScheme(s); err != nil {
		return nil, err
	}

	// create a fake client to mock API calls.
	cl := fake.NewClientBuilder().
		WithRuntimeObjects(objs...).
		WithStatusSubresource(&powerv1alpha1.PowerNodeState{}).
		Build()
	state, err := podstate.NewState()
	if err != nil {
		return nil, err
	}

	// create a ReconcileNode object with the scheme and fake client.
	mockPowerLibrary := new(hostMock)

	// Set up mock to return valid pools for profiles that should work
	// Each pool returns an empty CPUList (new CPUs will be added)
	mockPowerLibrary.On("GetExclusivePool", "performance").Return(createMockPoolWithCPUs([]uint{}))
	mockPowerLibrary.On("GetExclusivePool", "balance-performance").Return(createMockPoolWithCPUs([]uint{}))
	mockPowerLibrary.On("GetExclusivePool", "universal").Return(createMockPoolWithCPUs([]uint{}))
	mockPowerLibrary.On("GetExclusivePool", "zone-specific").Return(createMockPoolWithCPUs([]uint{}))
	// Return nil for profiles that should fail validation
	mockPowerLibrary.On("GetExclusivePool", "gpu-optimized").Return(nil)
	mockPowerLibrary.On("GetExclusivePool", "nonexistent").Return(nil)

	// Set up GetSharedPool with a broad range of CPUs.
	// Tests expect CPUs to be in the shared pool before moving to exclusive pools.
	// Include CPUs 0-99 to cover all test scenarios.
	sharedPoolCPUs := make([]uint, 100)
	for i := range sharedPoolCPUs {
		sharedPoolCPUs[i] = uint(i)
	}
	mockPowerLibrary.On("GetSharedPool").Return(createMockPoolWithCPUs(sharedPoolCPUs))

	r := &PowerPodReconciler{
		Client:             cl,
		Log:                ctrl.Log.WithName("testing"),
		Scheme:             s,
		State:              state,
		PodResourcesClient: *podResourcesClient,
		PowerLibrary:       mockPowerLibrary,
	}

	return r, nil
}

var defaultResources = corev1.ResourceRequirements{
	Limits: map[corev1.ResourceName]resource.Quantity{
		corev1.ResourceName("cpu"):    *resource.NewQuantity(3, resource.DecimalSI),
		corev1.ResourceName("memory"): *resource.NewQuantity(200, resource.DecimalSI),
		corev1.ResourceName("power.cluster-power-manager.github.io/performance"): *resource.NewQuantity(3, resource.DecimalSI),
	},
	Requests: map[corev1.ResourceName]resource.Quantity{
		corev1.ResourceName("cpu"):    *resource.NewQuantity(3, resource.DecimalSI),
		corev1.ResourceName("memory"): *resource.NewQuantity(200, resource.DecimalSI),
		corev1.ResourceName("power.cluster-power-manager.github.io/performance"): *resource.NewQuantity(3, resource.DecimalSI),
	},
}

var defaultProfile = &powerv1alpha1.PowerProfile{
	ObjectMeta: metav1.ObjectMeta{
		Name:      "performance",
		Namespace: PowerNamespace,
	},
	Spec: powerv1alpha1.PowerProfileSpec{},
}

var defaultPowerNodeState = &powerv1alpha1.PowerNodeState{
	ObjectMeta: metav1.ObjectMeta{
		Name:      "TestNode-power-state",
		Namespace: PowerNamespace,
	},
}

// runs through some basic cases for the controller with no errors
func TestPowerPod_Reconcile_Create(t *testing.T) {
	tcases := []struct {
		testCase        string
		nodeName        string
		podName         string
		podResources    []*podresourcesapi.PodResources
		clientObjs      []runtime.Object
		workloadToCores map[string][]uint
	}{
		{
			testCase: "Test Case 1 - Single container",
			nodeName: "TestNode",
			podName:  "test-pod-1",
			podResources: []*podresourcesapi.PodResources{
				{
					Name:      "test-pod-1",
					Namespace: PowerNamespace,
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:   "test-container-1",
							CpuIds: []int64{1, 5, 8},
						},
					},
				},
			},
			clientObjs: []runtime.Object{

				defaultPowerNodeState,
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "TestNode",
					},
				},
				defaultProfile,
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pod-1",
						Namespace: PowerNamespace,
						UID:       "abcdefg",
					},
					Spec: corev1.PodSpec{
						NodeName: "TestNode",
						Containers: []corev1.Container{
							{
								Name:      "test-container-1",
								Resources: defaultResources,
							},
						},
						EphemeralContainers: []corev1.EphemeralContainer{},
					},
					Status: corev1.PodStatus{
						Phase:    corev1.PodRunning,
						QOSClass: corev1.PodQOSGuaranteed,
						ContainerStatuses: []corev1.ContainerStatus{
							{
								Name:        "test-container-1",
								ContainerID: "docker://abcdefg",
							},
						},
					},
				},
			},
			workloadToCores: map[string][]uint{"performance-TestNode": {1, 5, 8}},
		},
		{
			testCase: "Test Case 2 - Two containers",
			nodeName: "TestNode",
			podName:  "test-pod-2",
			podResources: []*podresourcesapi.PodResources{
				{
					Name:      "test-pod-2",
					Namespace: PowerNamespace,
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:   "test-container-1",
							CpuIds: []int64{1, 2, 3},
						},
						{
							Name:   "test-container-2",
							CpuIds: []int64{4, 5, 6},
						},
					},
				},
			},
			clientObjs: []runtime.Object{

				defaultPowerNodeState,
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "TestNode",
					},
				},
				defaultProfile,

				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pod-2",
						Namespace: PowerNamespace,
						UID:       "abcdefg",
					},
					Spec: corev1.PodSpec{
						NodeName: "TestNode",
						Containers: []corev1.Container{
							{
								Name:      "test-container-1",
								Resources: defaultResources,
							},
							{
								Name:      "test-container-2",
								Resources: defaultResources,
							},
						},
						EphemeralContainers: []corev1.EphemeralContainer{},
					},
					Status: corev1.PodStatus{
						Phase:    corev1.PodRunning,
						QOSClass: corev1.PodQOSGuaranteed,
						ContainerStatuses: []corev1.ContainerStatus{
							{
								Name:        "example-container-1",
								ContainerID: "docker://abcdefg",
							},
							{
								Name:        "example-container-2",
								ContainerID: "docker://hijklmnop",
							},
						},
					},
				},
			},
			workloadToCores: map[string][]uint{"performance-TestNode": {1, 2, 3, 4, 5, 6}},
		},
		{
			testCase: "Test Case 3 - More Than One Profile",
			nodeName: "TestNode",
			podName:  "test-pod-1",
			podResources: []*podresourcesapi.PodResources{
				{
					Name:      "test-pod-1",
					Namespace: PowerNamespace,
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:   "test-container-1",
							CpuIds: []int64{1, 2, 3},
						},
						{
							Name:   "test-container-2",
							CpuIds: []int64{4, 5, 6},
						},
					},
				},
			},
			clientObjs: []runtime.Object{
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "TestNode",
					},
				},

				defaultPowerNodeState,
				defaultProfile,

				&powerv1alpha1.PowerProfile{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "balance-performance",
						Namespace: PowerNamespace,
					},
					Spec: powerv1alpha1.PowerProfileSpec{},
				},
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pod-1",
						Namespace: PowerNamespace,
						UID:       "abcdefg",
					},
					Spec: corev1.PodSpec{
						NodeName: "TestNode",
						Containers: []corev1.Container{
							{
								Name:      "test-container-1",
								Resources: defaultResources,
							},
							{
								Name: "test-container-2",
								Resources: corev1.ResourceRequirements{
									Limits: map[corev1.ResourceName]resource.Quantity{
										corev1.ResourceName("cpu"):    *resource.NewQuantity(3, resource.DecimalSI),
										corev1.ResourceName("memory"): *resource.NewQuantity(200, resource.DecimalSI),
										corev1.ResourceName("power.cluster-power-manager.github.io/balance-performance"): *resource.NewQuantity(3, resource.DecimalSI),
									},
									Requests: map[corev1.ResourceName]resource.Quantity{
										corev1.ResourceName("cpu"):    *resource.NewQuantity(3, resource.DecimalSI),
										corev1.ResourceName("memory"): *resource.NewQuantity(200, resource.DecimalSI),
										corev1.ResourceName("power.cluster-power-manager.github.io/balance-performance"): *resource.NewQuantity(3, resource.DecimalSI),
									},
								},
							},
						},
						EphemeralContainers: []corev1.EphemeralContainer{},
					},
					Status: corev1.PodStatus{
						Phase:    corev1.PodRunning,
						QOSClass: corev1.PodQOSGuaranteed,
						ContainerStatuses: []corev1.ContainerStatus{
							{
								Name:        "example-container-1",
								ContainerID: "docker://abcdefg",
							},
							{
								Name:        "example-container-2",
								ContainerID: "docker://abcdefg",
							},
						},
					},
				},
			},
			workloadToCores: map[string][]uint{"performance-TestNode": {1, 2, 3}, "balance-performance-TestNode": {4, 5, 6}},
		},
	}

	for _, tc := range tcases {
		t.Setenv("NODE_NAME", tc.nodeName)

		podResourcesClient := createFakePodResourcesListerClient(tc.podResources)

		r, err := createPodReconcilerObject(tc.clientObjs, podResourcesClient)
		assert.Nil(t, err)

		req := reconcile.Request{
			NamespacedName: client.ObjectKey{
				Name:      tc.podName,
				Namespace: PowerNamespace,
			},
		}

		_, err = r.Reconcile(context.TODO(), req)
		assert.Nil(t, err)

		// Verify PowerNodeState has the expected exclusive CPU entries.
		powerNodeState := &powerv1alpha1.PowerNodeState{}
		err = r.Client.Get(context.TODO(), client.ObjectKey{
			Name:      tc.nodeName + "-power-state",
			Namespace: PowerNamespace,
		}, powerNodeState)
		assert.Nil(t, err)

		// Collect all CPUs from the exclusive pools per profile.
		profileToCPUs := make(map[string][]uint)
		if powerNodeState.Status.CPUPools == nil {
			t.Fatal("expected CPUPools to be set in PowerNodeState")
		}
		for _, exclusive := range powerNodeState.Status.CPUPools.Exclusive {
			for _, container := range exclusive.PowerContainers {
				profileToCPUs[container.PowerProfile] = append(profileToCPUs[container.PowerProfile], container.CPUIDs...)
			}
		}

		// Verify expected CPUs per profile (key format: profile + "-" + nodeName).
		for workloadName, expectedCores := range tc.workloadToCores {
			profileName := workloadName[:len(workloadName)-len(tc.nodeName)-1]
			actualCores := profileToCPUs[profileName]
			sort.Slice(actualCores, func(i, j int) bool {
				return actualCores[i] < actualCores[j]
			})
			sort.Slice(expectedCores, func(i, j int) bool {
				return expectedCores[i] < expectedCores[j]
			})
			if !reflect.DeepEqual(expectedCores, actualCores) {
				t.Errorf("%s failed: expected CPU Ids for profile %s to be %v, got %v", tc.testCase, profileName, expectedCores, actualCores)
			}
		}
	}
}

// tests for error cases involving invalid pods
func TestPowerPod_Reconcile_ControllerErrors(t *testing.T) {
	tcases := []struct {
		testCase     string
		nodeName     string
		podName      string
		podResources []*podresourcesapi.PodResources
		clientObjs   []runtime.Object
		expectError  bool
	}{
		{
			testCase:    "Test Case 1 - Pod Not Running error",
			nodeName:    "TestNode",
			podName:     "test-pod-1",
			expectError: true,
			podResources: []*podresourcesapi.PodResources{
				{
					Name:      "test-pod-1",
					Namespace: PowerNamespace,
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:   "test-container-1",
							CpuIds: []int64{1, 2, 3},
						},
					},
				},
			},
			clientObjs: []runtime.Object{

				defaultPowerNodeState,
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "TestNode",
					},
				},
				defaultProfile,

				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pod-1",
						Namespace: PowerNamespace,
						UID:       "abcdefg",
					},
					Spec: corev1.PodSpec{
						NodeName: "TestNode",
						Containers: []corev1.Container{
							{
								Name:      "test-container-1",
								Resources: defaultResources,
							},
						},
						EphemeralContainers: []corev1.EphemeralContainer{},
					},
					Status: corev1.PodStatus{
						Phase:    corev1.PodPending,
						QOSClass: corev1.PodQOSGuaranteed,
						ContainerStatuses: []corev1.ContainerStatus{
							{
								Name:        "example-container-1",
								ContainerID: "docker://abcdefg",
							},
						},
					},
				},
			},
		},
		{
			testCase:    "Test Case 2 - No Pod UID error",
			nodeName:    "TestNode",
			podName:     "test-pod-1",
			expectError: true,
			podResources: []*podresourcesapi.PodResources{
				{
					Name:      "test-pod-1",
					Namespace: PowerNamespace,
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:   "test-container-1",
							CpuIds: []int64{1, 2, 3},
						},
					},
				},
			},
			clientObjs: []runtime.Object{

				defaultPowerNodeState,
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "TestNode",
					},
				},
				defaultProfile,

				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pod-1",
						Namespace: PowerNamespace,
					},
					Spec: corev1.PodSpec{
						NodeName: "TestNode",
						Containers: []corev1.Container{
							{
								Name:      "test-container-1",
								Resources: defaultResources,
							},
						},
						EphemeralContainers: []corev1.EphemeralContainer{},
					},
					Status: corev1.PodStatus{
						Phase:    corev1.PodRunning,
						QOSClass: corev1.PodQOSGuaranteed,
						ContainerStatuses: []corev1.ContainerStatus{
							{
								Name:        "example-container-1",
								ContainerID: "docker://abcdefg",
							},
						},
					},
				},
			},
		},
		{
			testCase:    "Test Case 3 - Resource Mismatch error",
			nodeName:    "TestNode",
			podName:     "test-pod-1",
			expectError: false,
			podResources: []*podresourcesapi.PodResources{
				{
					Name:      "test-pod-1",
					Namespace: PowerNamespace,
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:   "test-container-1",
							CpuIds: []int64{1, 2, 3},
						},
					},
				},
			},
			clientObjs: []runtime.Object{

				defaultPowerNodeState,
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "TestNode",
					},
				},
				defaultProfile,

				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pod-1",
						Namespace: PowerNamespace,
						UID:       "abcdefg",
					},
					Spec: corev1.PodSpec{
						NodeName: "TestNode",
						Containers: []corev1.Container{
							{
								Name: "test-container-1",
								Resources: corev1.ResourceRequirements{
									Limits: map[corev1.ResourceName]resource.Quantity{
										corev1.ResourceName("cpu"):    *resource.NewQuantity(3, resource.DecimalSI),
										corev1.ResourceName("memory"): *resource.NewQuantity(200, resource.DecimalSI),
										corev1.ResourceName("power.cluster-power-manager.github.io/performance"): *resource.NewQuantity(2, resource.DecimalSI),
									},
									Requests: map[corev1.ResourceName]resource.Quantity{
										corev1.ResourceName("cpu"):    *resource.NewQuantity(3, resource.DecimalSI),
										corev1.ResourceName("memory"): *resource.NewQuantity(200, resource.DecimalSI),
										corev1.ResourceName("power.cluster-power-manager.github.io/performance"): *resource.NewQuantity(2, resource.DecimalSI),
									},
								},
							},
						},
						EphemeralContainers: []corev1.EphemeralContainer{},
					},
					Status: corev1.PodStatus{
						Phase:    corev1.PodRunning,
						QOSClass: corev1.PodQOSGuaranteed,
						ContainerStatuses: []corev1.ContainerStatus{
							{
								Name:        "example-container-1",
								ContainerID: "docker://abcdefg",
							},
						},
					},
				},
			},
		},
		{
			testCase:    "Test Case 4 - Profile CR Does Not Exist error",
			nodeName:    "TestNode",
			podName:     "test-pod-1",
			expectError: true,
			podResources: []*podresourcesapi.PodResources{
				{
					Name:      "test-pod-1",
					Namespace: PowerNamespace,
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:   "test-container-1",
							CpuIds: []int64{1, 2, 3},
						},
					},
				},
			},
			clientObjs: []runtime.Object{

				defaultPowerNodeState,
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "TestNode",
					},
				},
				&powerv1alpha1.PowerProfile{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "balance-performance",
						Namespace: PowerNamespace,
					},
					Spec: powerv1alpha1.PowerProfileSpec{},
				},

				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pod-1",
						Namespace: PowerNamespace,
						UID:       "abcdefg",
					},
					Spec: corev1.PodSpec{
						NodeName: "TestNode",
						Containers: []corev1.Container{
							{
								Name:      "test-container-1",
								Resources: defaultResources,
							},
						},
						EphemeralContainers: []corev1.EphemeralContainer{},
					},
					Status: corev1.PodStatus{
						Phase:    corev1.PodRunning,
						QOSClass: corev1.PodQOSGuaranteed,
						ContainerStatuses: []corev1.ContainerStatus{
							{
								Name:        "example-container-1",
								ContainerID: "docker://abcdefg",
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range tcases {
		t.Setenv("NODE_NAME", tc.nodeName)

		podResourcesClient := createFakePodResourcesListerClient(tc.podResources)

		r, err := createPodReconcilerObject(tc.clientObjs, podResourcesClient)
		assert.Nil(t, err)

		req := reconcile.Request{
			NamespacedName: client.ObjectKey{
				Name:      tc.podName,
				Namespace: PowerNamespace,
			},
		}

		_, err = r.Reconcile(context.TODO(), req)
		if tc.expectError && err == nil {
			t.Errorf("%s failed: expected the pod controller to have failed", tc.testCase)
		} else if !tc.expectError && err != nil {
			t.Errorf("%s failed: expected no error but got: %v", tc.testCase, err)
		}
	}
}

func TestPowerPod_Reconcile_ControllerReturningNil(t *testing.T) {
	tcases := []struct {
		testCase     string
		nodeName     string
		podName      string
		namespace    string
		podResources []*podresourcesapi.PodResources
		clientObjs   []runtime.Object
		expectErr    bool
	}{
		{
			testCase:  "Test Case 1 - Not Exclusive Pod error",
			nodeName:  "TestNode",
			podName:   "test-pod-1",
			namespace: PowerNamespace,
			podResources: []*podresourcesapi.PodResources{
				{
					Name:      "test-pod-1",
					Namespace: PowerNamespace,
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:   "test-container-1",
							CpuIds: []int64{1, 2, 3},
						},
					},
				},
			},
			clientObjs: []runtime.Object{
				defaultPowerNodeState,
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "TestNode",
					},
				},
				defaultProfile,

				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pod-1",
						Namespace: PowerNamespace,
						UID:       "abcdefg",
					},
					Spec: corev1.PodSpec{
						NodeName: "TestNode",
						Containers: []corev1.Container{
							{
								Name: "test-container-1",
								Resources: corev1.ResourceRequirements{
									Limits: map[corev1.ResourceName]resource.Quantity{
										corev1.ResourceName("cpu"):    *resource.NewQuantity(3, resource.DecimalSI),
										corev1.ResourceName("memory"): *resource.NewQuantity(200, resource.DecimalSI),
										corev1.ResourceName("power.cluster-power-manager.github.io/performance"): *resource.NewQuantity(3, resource.DecimalSI),
									},
									Requests: map[corev1.ResourceName]resource.Quantity{
										corev1.ResourceName("cpu"): *resource.NewQuantity(3, resource.DecimalSI),
										corev1.ResourceName("power.cluster-power-manager.github.io/performance"): *resource.NewQuantity(3, resource.DecimalSI),
									},
								},
							},
						},
						EphemeralContainers: []corev1.EphemeralContainer{},
					},
					Status: corev1.PodStatus{
						Phase:    corev1.PodRunning,
						QOSClass: corev1.PodQOSBurstable,
						ContainerStatuses: []corev1.ContainerStatus{
							{
								Name:        "example-container-1",
								ContainerID: "docker://abcdefg",
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range tcases {
		t.Setenv("NODE_NAME", tc.nodeName)

		podResourcesClient := createFakePodResourcesListerClient(tc.podResources)

		r, err := createPodReconcilerObject(tc.clientObjs, podResourcesClient)
		assert.Nil(t, err)

		req := reconcile.Request{
			NamespacedName: client.ObjectKey{
				Name:      tc.podName,
				Namespace: tc.namespace,
			},
		}

		_, err = r.Reconcile(context.TODO(), req)
		if tc.expectErr {
			assert.NotNil(t, err, tc.testCase)
		} else {
			assert.Nil(t, err, tc.testCase)
		}
	}
}

// ensures CPUs are moved back to shared pool upon pod deletion
func TestPowerPod_Reconcile_Delete(t *testing.T) {
	tcases := []struct {
		testCase      string
		nodeName      string
		podName       string
		podResources  []*podresourcesapi.PodResources
		clientObjs    []runtime.Object
		guaranteedPod powerv1alpha1.GuaranteedPod
	}{
		{
			testCase: "Test Case 1: Single Container",
			nodeName: "TestNode",
			podName:  "test-pod-1",
			podResources: []*podresourcesapi.PodResources{
				{
					Name:      "test-pod-1",
					Namespace: PowerNamespace,
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:   "test-container-1",
							CpuIds: []int64{1, 2, 3},
						},
					},
				},
			},
			clientObjs: []runtime.Object{
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "TestNode",
					},
				},
				// PowerNodeState with the pod's exclusive CPU info (for deletion lookup)
				&powerv1alpha1.PowerNodeState{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "TestNode-power-state",
						Namespace: PowerNamespace,
					},
					Status: powerv1alpha1.PowerNodeStateStatus{
						CPUPools: &powerv1alpha1.CPUPoolsStatus{
							Exclusive: []powerv1alpha1.ExclusiveCPUPoolStatus{
								{
									PodUID: "abcdefg",
									Pod:    "test-pod-1",
									PowerContainers: []powerv1alpha1.PowerContainer{
										{
											Name:         "test-container-1",
											ID:           "abcdefg",
											PowerProfile: "performance",
											CPUIDs:       []uint{1, 2, 3},
										},
									},
								},
							},
						},
					},
				},
				defaultProfile,
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "test-pod-1",
						Namespace:         PowerNamespace,
						UID:               "abcdefg",
						DeletionTimestamp: &metav1.Time{Time: time.Date(9999, time.Month(1), 21, 1, 10, 30, 0, time.UTC)},
						Finalizers:        []string{"power.cluster-power-manager.github.io/finalizer"},
					},
					Spec: corev1.PodSpec{
						NodeName: "TestNode",
					},
				},
			},
			guaranteedPod: powerv1alpha1.GuaranteedPod{
				Node:      "TestNode",
				Name:      "test-pod-1",
				Namespace: PowerNamespace,
				UID:       "abcdefg",
				Containers: []powerv1alpha1.Container{
					{
						Name:          "test-container-1",
						ID:            "abcdefg",
						Pod:           "test-pod-1",
						ExclusiveCPUs: []uint{1, 2, 3},
						PowerProfile:  "performance",
					},
				},
			},
		},
	}

	for _, tc := range tcases {
		t.Setenv("NODE_NAME", tc.nodeName)

		podResourcesClient := createFakePodResourcesListerClient(tc.podResources)

		r, err := createPodReconcilerObject(tc.clientObjs, podResourcesClient)
		assert.Nil(t, err)

		err = r.State.UpdateStateGuaranteedPods(tc.guaranteedPod)
		assert.Nil(t, err)

		req := reconcile.Request{
			NamespacedName: client.ObjectKey{
				Name:      tc.podName,
				Namespace: PowerNamespace,
			},
		}

		_, err = r.Reconcile(context.TODO(), req)
		assert.Nil(t, err)

		// Verify the pod was removed from internal state
		podFromState := r.State.GetPodFromState(tc.podName, PowerNamespace)
		assert.Empty(t, podFromState.UID, "%s: expected pod to be removed from internal state", tc.testCase)

		// Verify the shared pool MoveCPUIDs was called (CPUs moved back)
		// The mock records all calls, so we verify it was called
		mockHost := r.PowerLibrary.(*hostMock)
		mockHost.AssertCalled(t, "GetSharedPool")

		// Note: The fake client doesn't fully support SSA semantics for removal.
		// In a real cluster, the SSA patch with empty containers would remove the
		// pod's entry from PowerNodeState. Here we verify the reconcile succeeds
		// and trust the SSA behavior works correctly with the real API server.
	}
}

// uses errclient to mock errors from the client
func TestPowerPod_Reconcile_PodClientErrs(t *testing.T) {
	var deletedPod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-pod-1",
			Namespace:         PowerNamespace,
			UID:               "abcdefg",
			DeletionTimestamp: &metav1.Time{Time: time.Date(9999, time.Month(1), 21, 1, 10, 30, 0, time.UTC)},
			Finalizers:        []string{"power.cluster-power-manager.github.io/finalizer"},
		},
		Spec: corev1.PodSpec{
			NodeName: "TestNode",
		},
	}
	var defaultPod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-1",
			Namespace: PowerNamespace,
			UID:       "abcdefg",
		},
		Spec: corev1.PodSpec{
			NodeName: "TestNode",
			Containers: []corev1.Container{
				{
					Name:      "test-container-1",
					Resources: defaultResources,
				},
			},
			EphemeralContainers: []corev1.EphemeralContainer{},
		},
		Status: corev1.PodStatus{
			Phase:    corev1.PodRunning,
			QOSClass: corev1.PodQOSGuaranteed,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:        "test-container-1",
					ContainerID: "docker://abcdefg",
				},
			},
		},
	}
	tcases := []struct {
		testCase      string
		nodeName      string
		podName       string
		powerNodeName string
		convertClient func(client.Client) client.Client
		clientErr     string
		podResources  []*podresourcesapi.PodResources
		guaranteedPod powerv1alpha1.GuaranteedPod
	}{
		{
			testCase: "Test Case 1 - Invalid Get requests",
			nodeName: "TestNode",
			podName:  "test-pod-1",
			convertClient: func(c client.Client) client.Client {
				mkcl := new(errClient)
				mkcl.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("client get error"))
				return mkcl
			},
			clientErr:    "client get error",
			podResources: []*podresourcesapi.PodResources{},
		},
		{
			testCase: "Test Case 2 - Invalid Update requests",
			nodeName: "TestNode",
			podName:  "test-pod-1",
			convertClient: func(c client.Client) client.Client {
				mkcl := new(errClient)
				mkcl.On("Status").Return(&errSubResourceClient{mkcl})
				mkcl.On("Get", mock.Anything, mock.Anything, mock.AnythingOfType("*v1.Pod")).Return(nil).Run(func(args mock.Arguments) {
					pod := args.Get(2).(*corev1.Pod)
					*pod = *deletedPod
				})
				mkcl.On("Get", mock.Anything, mock.Anything, mock.AnythingOfType("*v1alpha1.PowerNodeState")).Return(nil).Run(func(args mock.Arguments) {
					nodeState := args.Get(2).(*powerv1alpha1.PowerNodeState)
					*nodeState = powerv1alpha1.PowerNodeState{
						ObjectMeta: metav1.ObjectMeta{
							ManagedFields: []metav1.ManagedFieldsEntry{
								{Manager: "powerpod-controller.abcdefg", Operation: metav1.ManagedFieldsOperationApply},
							},
						},
						Status: powerv1alpha1.PowerNodeStateStatus{
							CPUPools: &powerv1alpha1.CPUPoolsStatus{
								Exclusive: []powerv1alpha1.ExclusiveCPUPoolStatus{
									{
										PodUID: "abcdefg",
										Pod:    "test-pod-1",
										PowerContainers: []powerv1alpha1.PowerContainer{
											{
												Name:         "test-container-1",
												ID:           "abcdefg",
												PowerProfile: "performance",
												CPUIDs:       []uint{1, 2, 3},
											},
										},
									},
								},
							},
						},
					}
				})
				return mkcl
			},
			clientErr: "client Patch error",
			podResources: []*podresourcesapi.PodResources{
				{
					Name:      "test-pod-1",
					Namespace: PowerNamespace,
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:   "test-container-1",
							CpuIds: []int64{1, 2, 3},
						},
					},
				},
			},
			guaranteedPod: powerv1alpha1.GuaranteedPod{
				Node:      "TestNode",
				Name:      "test-pod-1",
				Namespace: PowerNamespace,
				UID:       "abcdefg",
				Containers: []powerv1alpha1.Container{
					{
						Name:          "test-container-1",
						ID:            "abcdefg",
						Pod:           "test-pod-1",
						ExclusiveCPUs: []uint{1, 2, 3},
						PowerProfile:  "performance",
					},
				},
			},
		},
		{
			testCase: "Test Case 3 - Invalid List requests",
			nodeName: "TestNode",
			podName:  "test-pod-1",
			convertClient: func(c client.Client) client.Client {
				mkcl := new(errClient)
				// Use a success status writer since we expect no error from this test
				statusWriter := new(mockResourceWriter)
				statusWriter.On("Patch", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
				mkcl.On("Status").Return(statusWriter)
				mkcl.On("Get", mock.Anything, mock.Anything, mock.AnythingOfType("*v1.Pod")).Return(nil).Run(func(args mock.Arguments) {
					node := args.Get(2).(*corev1.Pod)
					*node = *defaultPod
				})
				mkcl.On("Get", mock.Anything, mock.Anything, mock.AnythingOfType("*v1alpha1.PowerProfile")).Return(fmt.Errorf("powerprofiles.power.cluster-power-manager.github.io \"performance\" not found"))
				return mkcl
			},
			clientErr: "",
			podResources: []*podresourcesapi.PodResources{
				{
					Name:      "test-pod-1",
					Namespace: PowerNamespace,
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:   "test-container-1",
							CpuIds: []int64{1, 5, 8},
						},
					},
				},
			},
		},
	}
	for _, tc := range tcases {
		t.Setenv("NODE_NAME", tc.nodeName)
		podResourcesClient := createFakePodResourcesListerClient(tc.podResources)

		r, err := createPodReconcilerObject([]runtime.Object{}, podResourcesClient)
		assert.Nil(t, err)
		err = r.State.UpdateStateGuaranteedPods(tc.guaranteedPod)
		assert.Nil(t, err)
		req := reconcile.Request{
			NamespacedName: client.ObjectKey{
				Name:      tc.podName,
				Namespace: PowerNamespace,
			},
		}
		r.Client = tc.convertClient(r.Client)
		_, err = r.Reconcile(context.TODO(), req)
		if tc.clientErr == "" {
			assert.NoError(t, err)
		} else {
			assert.ErrorContains(t, err, tc.clientErr)
		}

	}

}

func TestPowerPod_ControlPLaneSocket(t *testing.T) {
	tcases := []struct {
		testCase     string
		nodeName     string
		podName      string
		podResources []*podresourcesapi.PodResources
		clientObjs   []runtime.Object
		validateErr  func(t *testing.T, e error)
	}{
		{
			testCase: "Using control plane socket",
			nodeName: "TestNode",
			podName:  "test-pod-1",
			validateErr: func(t *testing.T, err error) {
				assert.Nil(t, err)
			},
			podResources: []*podresourcesapi.PodResources{
				{
					Name:      "test-pod-1",
					Namespace: PowerNamespace,
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:   "test-container-1",
							CpuIds: []int64{1, 5, 8},
						},
					},
				},
			},
			clientObjs: []runtime.Object{

				defaultPowerNodeState,
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "TestNode",
					},
				},
				defaultProfile,
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pod-1",
						Namespace: PowerNamespace,
						UID:       "abcdefg",
					},
					Spec: corev1.PodSpec{
						NodeName: "TestNode",
						Containers: []corev1.Container{
							{
								Name: "test-container-1",
								Resources: corev1.ResourceRequirements{
									Limits: map[corev1.ResourceName]resource.Quantity{
										corev1.ResourceName("power.cluster-power-manager.github.io/performance"): *resource.NewQuantity(3, resource.DecimalSI),
									},
									Requests: map[corev1.ResourceName]resource.Quantity{
										corev1.ResourceName("power.cluster-power-manager.github.io/performance"): *resource.NewQuantity(3, resource.DecimalSI),
									},
									Claims: []corev1.ResourceClaim{{Name: "test-claim"}},
								},
							},
						},
						EphemeralContainers: []corev1.EphemeralContainer{},
					},
					Status: corev1.PodStatus{
						Phase:    corev1.PodRunning,
						QOSClass: corev1.PodQOSBestEffort,
						ContainerStatuses: []corev1.ContainerStatus{
							{
								Name:        "test-container-1",
								ContainerID: "docker://abcdefg",
							},
						},
					},
				},
			},
		},
		{
			testCase: "Mismatched cores/requests",
			nodeName: "TestNode",
			podName:  "test-pod-1",
			validateErr: func(t *testing.T, err error) {
				assert.ErrorContains(t, err, "recoverable errors")
			},
			podResources: []*podresourcesapi.PodResources{
				{
					Name:      "test-pod-1",
					Namespace: PowerNamespace,
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:   "test-container-1",
							CpuIds: []int64{1, 5, 8},
						},
					},
				},
			},
			clientObjs: []runtime.Object{

				defaultPowerNodeState,
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "TestNode",
					},
				},
				defaultProfile,
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pod-1",
						Namespace: PowerNamespace,
						UID:       "abcdefg",
					},
					Spec: corev1.PodSpec{
						NodeName: "TestNode",
						Containers: []corev1.Container{
							{
								Name: "test-container-1",
								Resources: corev1.ResourceRequirements{
									Limits: map[corev1.ResourceName]resource.Quantity{
										corev1.ResourceName("power.cluster-power-manager.github.io/performance"): *resource.NewQuantity(2, resource.DecimalSI),
									},
									Requests: map[corev1.ResourceName]resource.Quantity{
										corev1.ResourceName("power.cluster-power-manager.github.io/performance"): *resource.NewQuantity(2, resource.DecimalSI),
									},
									Claims: []corev1.ResourceClaim{{Name: "test-claim"}},
								},
							},
						},
						EphemeralContainers: []corev1.EphemeralContainer{},
					},
					Status: corev1.PodStatus{
						Phase:    corev1.PodRunning,
						QOSClass: corev1.PodQOSBestEffort,
						ContainerStatuses: []corev1.ContainerStatus{
							{
								Name:        "test-container-1",
								ContainerID: "docker://abcdefg",
							},
						},
					},
				},
			},
		},
	}
	for i, tc := range tcases {
		t.Logf("Test Case %d: %s", i+1, tc.testCase)
		t.Setenv("NODE_NAME", tc.nodeName)

		podResourcesClient := createFakePodResourcesListerClient(tc.podResources)

		r, err := createPodReconcilerObject(tc.clientObjs, podResourcesClient)
		assert.Nil(t, err)

		req := reconcile.Request{
			NamespacedName: client.ObjectKey{
				Name:      tc.podName,
				Namespace: PowerNamespace,
			},
		}

		_, err = r.Reconcile(context.TODO(), req)
		tc.validateErr(t, err)

	}
}

// tests positive and negative cases for SetupWithManager function
func TestPowerPod_Reconcile_SetupPass(t *testing.T) {
	podResources := []*podresourcesapi.PodResources{}
	podResourcesClient := createFakePodResourcesListerClient(podResources)
	r, err := createPodReconcilerObject([]runtime.Object{}, podResourcesClient)
	assert.Nil(t, err)
	mgr := new(mgrMock)
	mgr.On("GetControllerOptions").Return(config.Controller{})
	mgr.On("GetScheme").Return(r.Scheme)
	mgr.On("GetLogger").Return(r.Log)
	mgr.On("SetFields", mock.Anything).Return(nil)
	mgr.On("Add", mock.Anything).Return(nil)
	mgr.On("GetCache").Return(new(cacheMk))
	mgr.On("GetFieldIndexer").Return(&fieldIndexerMock{})
	err = (&PowerPodReconciler{
		Client: r.Client,
		Scheme: r.Scheme,
	}).SetupWithManager(mgr)
	assert.Nil(t, err)

}

func TestPowerPod_Reconcile_SetupFail(t *testing.T) {
	podResources := []*podresourcesapi.PodResources{}
	podResourcesClient := createFakePodResourcesListerClient(podResources)
	r, err := createPodReconcilerObject([]runtime.Object{}, podResourcesClient)
	assert.Nil(t, err)
	mgr := new(mgrMock)
	mgr.On("GetControllerOptions").Return(config.Controller{})
	mgr.On("GetScheme").Return(r.Scheme)
	mgr.On("GetLogger").Return(r.Log)
	mgr.On("GetFieldIndexer").Return(&fieldIndexerMock{})
	mgr.On("Add", mock.Anything).Return(fmt.Errorf("setup fail"))
	err = (&PowerPodReconciler{
		Client: r.Client,
		Scheme: r.Scheme,
	}).SetupWithManager(mgr)
	assert.Error(t, err)

}

func TestPowerPod_ValidateProfileNodeSelectorMatching(t *testing.T) {
	testNode := "TestNode"

	baseNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: testNode,
			Labels: map[string]string{
				"node-type": "worker",
				"zone":      "us-west-1a",
				"env":       "production",
			},
		},
	}

	basePowerNodeState := &powerv1alpha1.PowerNodeState{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testNode + "-power-state",
			Namespace: PowerNamespace,
		},
	}

	matchingProfile := &powerv1alpha1.PowerProfile{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "performance",
			Namespace: PowerNamespace,
		},
		Spec: powerv1alpha1.PowerProfileSpec{
			NodeSelector: powerv1alpha1.NodeSelector{
				LabelSelector: metav1.LabelSelector{
					MatchLabels: map[string]string{
						"node-type": "worker",
					},
				},
			},
		},
	}

	nonMatchingProfile := &powerv1alpha1.PowerProfile{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "gpu-optimized",
			Namespace: PowerNamespace,
		},
		Spec: powerv1alpha1.PowerProfileSpec{
			NodeSelector: powerv1alpha1.NodeSelector{
				LabelSelector: metav1.LabelSelector{
					MatchLabels: map[string]string{
						"node-type": "gpu-node",
					},
				},
			},
		},
	}

	universalProfile := &powerv1alpha1.PowerProfile{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "universal",
			Namespace: PowerNamespace,
		},
		Spec: powerv1alpha1.PowerProfileSpec{},
	}

	expressionMatchingProfile := &powerv1alpha1.PowerProfile{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "zone-specific",
			Namespace: PowerNamespace,
		},
		Spec: powerv1alpha1.PowerProfileSpec{
			NodeSelector: powerv1alpha1.NodeSelector{
				LabelSelector: metav1.LabelSelector{
					MatchExpressions: []metav1.LabelSelectorRequirement{
						{
							Key:      "zone",
							Operator: metav1.LabelSelectorOpIn,
							Values:   []string{"us-west-1a", "us-west-1b"},
						},
						{
							Key:      "env",
							Operator: metav1.LabelSelectorOpExists,
						},
					},
				},
			},
		},
	}

	testCases := []struct {
		name               string
		nodeLabels         map[string]string
		podSpec            corev1.PodSpec
		podStatus          corev1.PodStatus
		profiles           []runtime.Object
		podResources       []*podresourcesapi.PodResources
		expectError        bool
		expectRecoverable  bool
		errorContains      string
		expectedContainers []powerv1alpha1.PowerContainer
	}{
		{
			name: "Single profile with matching node selector",
			nodeLabels: map[string]string{
				"node-type": "worker",
				"zone":      "us-west-1a",
			},
			podSpec: corev1.PodSpec{
				NodeName: testNode,
				Containers: []corev1.Container{
					{
						Name: "test-container",
						Resources: corev1.ResourceRequirements{
							Limits: map[corev1.ResourceName]resource.Quantity{
								"cpu": *resource.NewQuantity(2, resource.DecimalSI),
								"power.cluster-power-manager.github.io/performance": *resource.NewQuantity(2, resource.DecimalSI),
							},
							Requests: map[corev1.ResourceName]resource.Quantity{
								"cpu": *resource.NewQuantity(2, resource.DecimalSI),
								"power.cluster-power-manager.github.io/performance": *resource.NewQuantity(2, resource.DecimalSI),
							},
						},
					},
				},
			},
			podStatus: corev1.PodStatus{
				Phase:    corev1.PodRunning,
				QOSClass: corev1.PodQOSGuaranteed,
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name:        "test-container",
						ContainerID: "docker://abc123",
					},
				},
			},
			profiles: []runtime.Object{matchingProfile},
			podResources: []*podresourcesapi.PodResources{
				{
					Name:      "test-pod",
					Namespace: PowerNamespace,
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:   "test-container",
							CpuIds: []int64{1, 2},
						},
					},
				},
			},
			expectError: false,
			expectedContainers: []powerv1alpha1.PowerContainer{
				{Name: "test-container", PowerProfile: "performance", CPUIDs: []uint{1, 2}},
			},
		},
		{
			name: "Profile with non-matching node selector",
			nodeLabels: map[string]string{
				"node-type": "worker",
				"zone":      "us-west-1a",
			},
			podSpec: corev1.PodSpec{
				NodeName: testNode,
				Containers: []corev1.Container{
					{
						Name: "test-container",
						Resources: corev1.ResourceRequirements{
							Limits: map[corev1.ResourceName]resource.Quantity{
								"cpu": *resource.NewQuantity(2, resource.DecimalSI),
								"power.cluster-power-manager.github.io/gpu-optimized": *resource.NewQuantity(2, resource.DecimalSI),
							},
							Requests: map[corev1.ResourceName]resource.Quantity{
								"cpu": *resource.NewQuantity(2, resource.DecimalSI),
								"power.cluster-power-manager.github.io/gpu-optimized": *resource.NewQuantity(2, resource.DecimalSI),
							},
						},
					},
				},
			},
			podStatus: corev1.PodStatus{
				Phase:    corev1.PodRunning,
				QOSClass: corev1.PodQOSGuaranteed,
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name:        "test-container",
						ContainerID: "docker://abc123",
					},
				},
			},
			profiles: []runtime.Object{nonMatchingProfile},
			podResources: []*podresourcesapi.PodResources{
				{
					Name:      "test-pod",
					Namespace: PowerNamespace,
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:   "test-container",
							CpuIds: []int64{1, 2},
						},
					},
				},
			},
			expectError:       true,
			expectRecoverable: true,
			errorContains: fmt.Sprintf(
				"recoverable errors encountered: power profile '%s' is not available on node %s",
				nonMatchingProfile.Name,
				testNode,
			),
		},
		{
			name: "Universal profile with no node selector",
			nodeLabels: map[string]string{
				"node-type": "worker",
				"zone":      "us-west-1a",
			},
			podSpec: corev1.PodSpec{
				NodeName: testNode,
				Containers: []corev1.Container{
					{
						Name: "test-container",
						Resources: corev1.ResourceRequirements{
							Limits: map[corev1.ResourceName]resource.Quantity{
								"cpu": *resource.NewQuantity(2, resource.DecimalSI),
								"power.cluster-power-manager.github.io/universal": *resource.NewQuantity(2, resource.DecimalSI),
							},
							Requests: map[corev1.ResourceName]resource.Quantity{
								"cpu": *resource.NewQuantity(2, resource.DecimalSI),
								"power.cluster-power-manager.github.io/universal": *resource.NewQuantity(2, resource.DecimalSI),
							},
						},
					},
				},
			},
			podStatus: corev1.PodStatus{
				Phase:    corev1.PodRunning,
				QOSClass: corev1.PodQOSGuaranteed,
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name:        "test-container",
						ContainerID: "docker://abc123",
					},
				},
			},
			profiles: []runtime.Object{universalProfile},
			podResources: []*podresourcesapi.PodResources{
				{
					Name:      "test-pod",
					Namespace: PowerNamespace,
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:   "test-container",
							CpuIds: []int64{3, 4},
						},
					},
				},
			},
			expectError: false,
			expectedContainers: []powerv1alpha1.PowerContainer{
				{Name: "test-container", PowerProfile: "universal", CPUIDs: []uint{3, 4}},
			},
		},
		{
			name: "Profile with MatchExpressions selector",
			nodeLabels: map[string]string{
				"node-type": "worker",
				"zone":      "us-west-1a",
				"env":       "production",
			},
			podSpec: corev1.PodSpec{
				NodeName: testNode,
				Containers: []corev1.Container{
					{
						Name: "test-container",
						Resources: corev1.ResourceRequirements{
							Limits: map[corev1.ResourceName]resource.Quantity{
								"cpu": *resource.NewQuantity(2, resource.DecimalSI),
								"power.cluster-power-manager.github.io/zone-specific": *resource.NewQuantity(2, resource.DecimalSI),
							},
							Requests: map[corev1.ResourceName]resource.Quantity{
								"cpu": *resource.NewQuantity(2, resource.DecimalSI),
								"power.cluster-power-manager.github.io/zone-specific": *resource.NewQuantity(2, resource.DecimalSI),
							},
						},
					},
				},
			},
			podStatus: corev1.PodStatus{
				Phase:    corev1.PodRunning,
				QOSClass: corev1.PodQOSGuaranteed,
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name:        "test-container",
						ContainerID: "docker://abc123",
					},
				},
			},
			profiles: []runtime.Object{expressionMatchingProfile},
			podResources: []*podresourcesapi.PodResources{
				{
					Name:      "test-pod",
					Namespace: PowerNamespace,
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:   "test-container",
							CpuIds: []int64{5, 6},
						},
					},
				},
			},
			expectError: false,
			expectedContainers: []powerv1alpha1.PowerContainer{
				{Name: "test-container", PowerProfile: "zone-specific", CPUIDs: []uint{5, 6}},
			},
		},
		{
			name: "Multiple containers with different profiles",
			nodeLabels: map[string]string{
				"node-type": "worker",
				"zone":      "us-west-1a",
				"env":       "production",
			},
			podSpec: corev1.PodSpec{
				NodeName: testNode,
				Containers: []corev1.Container{
					{
						Name: "performance-container",
						Resources: corev1.ResourceRequirements{
							Limits: map[corev1.ResourceName]resource.Quantity{
								"cpu": *resource.NewQuantity(2, resource.DecimalSI),
								"power.cluster-power-manager.github.io/performance": *resource.NewQuantity(2, resource.DecimalSI),
							},
							Requests: map[corev1.ResourceName]resource.Quantity{
								"cpu": *resource.NewQuantity(2, resource.DecimalSI),
								"power.cluster-power-manager.github.io/performance": *resource.NewQuantity(2, resource.DecimalSI),
							},
						},
					},
					{
						Name: "universal-container",
						Resources: corev1.ResourceRequirements{
							Limits: map[corev1.ResourceName]resource.Quantity{
								"cpu": *resource.NewQuantity(1, resource.DecimalSI),
								"power.cluster-power-manager.github.io/universal": *resource.NewQuantity(1, resource.DecimalSI),
							},
							Requests: map[corev1.ResourceName]resource.Quantity{
								"cpu": *resource.NewQuantity(1, resource.DecimalSI),
								"power.cluster-power-manager.github.io/universal": *resource.NewQuantity(1, resource.DecimalSI),
							},
						},
					},
				},
			},
			podStatus: corev1.PodStatus{
				Phase:    corev1.PodRunning,
				QOSClass: corev1.PodQOSGuaranteed,
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name:        "performance-container",
						ContainerID: "docker://abc123",
					},
					{
						Name:        "universal-container",
						ContainerID: "docker://def456",
					},
				},
			},
			profiles: []runtime.Object{matchingProfile, universalProfile},
			podResources: []*podresourcesapi.PodResources{
				{
					Name:      "test-pod",
					Namespace: PowerNamespace,
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:   "performance-container",
							CpuIds: []int64{7, 8},
						},
						{
							Name:   "universal-container",
							CpuIds: []int64{9},
						},
					},
				},
			},
			expectError: false,
			expectedContainers: []powerv1alpha1.PowerContainer{
				{Name: "performance-container", PowerProfile: "performance", CPUIDs: []uint{7, 8}},
				{Name: "universal-container", PowerProfile: "universal", CPUIDs: []uint{9}},
			},
		},
		{
			name: "Profile doesn't exist in cluster",
			nodeLabels: map[string]string{
				"node-type": "worker",
			},
			podSpec: corev1.PodSpec{
				NodeName: testNode,
				Containers: []corev1.Container{
					{
						Name: "test-container",
						Resources: corev1.ResourceRequirements{
							Limits: map[corev1.ResourceName]resource.Quantity{
								"cpu": *resource.NewQuantity(2, resource.DecimalSI),
								"power.cluster-power-manager.github.io/nonexistent": *resource.NewQuantity(2, resource.DecimalSI),
							},
							Requests: map[corev1.ResourceName]resource.Quantity{
								"cpu": *resource.NewQuantity(2, resource.DecimalSI),
								"power.cluster-power-manager.github.io/nonexistent": *resource.NewQuantity(2, resource.DecimalSI),
							},
						},
					},
				},
			},
			podStatus: corev1.PodStatus{
				Phase:    corev1.PodRunning,
				QOSClass: corev1.PodQOSGuaranteed,
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name:        "test-container",
						ContainerID: "docker://abc123",
					},
				},
			},
			profiles: []runtime.Object{
				&powerv1alpha1.PowerProfile{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "nonexistent",
						Namespace: PowerNamespace,
					},
					Spec: powerv1alpha1.PowerProfileSpec{
						NodeSelector: powerv1alpha1.NodeSelector{
							LabelSelector: metav1.LabelSelector{
								MatchLabels: map[string]string{
									"node-type": "worker",
								},
							},
						},
					},
				},
			},
			podResources: []*podresourcesapi.PodResources{
				{
					Name:      "test-pod",
					Namespace: PowerNamespace,
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:   "test-container",
							CpuIds: []int64{1, 2},
						},
					},
				},
			},
			expectError:       true,
			expectRecoverable: true,
			errorContains: fmt.Sprintf(
				"recoverable errors encountered: power profile 'nonexistent' is not available on node %s",
				testNode,
			),
		},
		{
			name: "Mixed scenario - one matching, one non-matching profile",
			nodeLabels: map[string]string{
				"node-type": "worker",
				"zone":      "us-west-1a",
			},
			podSpec: corev1.PodSpec{
				NodeName: testNode,
				Containers: []corev1.Container{
					{
						Name: "good-container",
						Resources: corev1.ResourceRequirements{
							Limits: map[corev1.ResourceName]resource.Quantity{
								"cpu": *resource.NewQuantity(2, resource.DecimalSI),
								"power.cluster-power-manager.github.io/performance": *resource.NewQuantity(2, resource.DecimalSI),
							},
							Requests: map[corev1.ResourceName]resource.Quantity{
								"cpu": *resource.NewQuantity(2, resource.DecimalSI),
								"power.cluster-power-manager.github.io/performance": *resource.NewQuantity(2, resource.DecimalSI),
							},
						},
					},
					{
						Name: "bad-container",
						Resources: corev1.ResourceRequirements{
							Limits: map[corev1.ResourceName]resource.Quantity{
								"cpu": *resource.NewQuantity(1, resource.DecimalSI),
								"power.cluster-power-manager.github.io/gpu-optimized": *resource.NewQuantity(1, resource.DecimalSI),
							},
							Requests: map[corev1.ResourceName]resource.Quantity{
								"cpu": *resource.NewQuantity(1, resource.DecimalSI),
								"power.cluster-power-manager.github.io/gpu-optimized": *resource.NewQuantity(1, resource.DecimalSI),
							},
						},
					},
				},
			},
			podStatus: corev1.PodStatus{
				Phase:    corev1.PodRunning,
				QOSClass: corev1.PodQOSGuaranteed,
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name:        "good-container",
						ContainerID: "docker://abc123",
					},
					{
						Name:        "bad-container",
						ContainerID: "docker://def456",
					},
				},
			},
			profiles: []runtime.Object{matchingProfile, nonMatchingProfile},
			podResources: []*podresourcesapi.PodResources{
				{
					Name:      "test-pod",
					Namespace: PowerNamespace,
					Containers: []*podresourcesapi.ContainerResources{
						{
							Name:   "good-container",
							CpuIds: []int64{10, 11},
						},
						{
							Name:   "bad-container",
							CpuIds: []int64{12},
						},
					},
				},
			},
			expectError:       true,
			expectRecoverable: true,
			errorContains: fmt.Sprintf(
				"recoverable errors encountered: power profile 'gpu-optimized' is not available on node %s",
				testNode,
			),
			expectedContainers: []powerv1alpha1.PowerContainer{
				{Name: "good-container", PowerProfile: "performance", CPUIDs: []uint{10, 11}},
				{Name: "bad-container", PowerProfile: "gpu-optimized", Errors: []string{
					fmt.Sprintf("power profile 'gpu-optimized' is not available on node %s", testNode),
				}},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("NODE_NAME", testNode)

			// Create node with specified labels
			node := baseNode.DeepCopy()
			node.Labels = tc.nodeLabels

			// Create pod with test spec
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: PowerNamespace,
					UID:       "test-uid-123",
				},
				Spec:   tc.podSpec,
				Status: tc.podStatus,
			}

			clientObjs := []runtime.Object{node, basePowerNodeState, pod}
			clientObjs = append(clientObjs, tc.profiles...)

			podResourcesClient := createFakePodResourcesListerClient(tc.podResources)

			r, err := createPodReconcilerObject(clientObjs, podResourcesClient)
			assert.NoError(t, err)

			req := reconcile.Request{
				NamespacedName: client.ObjectKey{
					Name:      "test-pod",
					Namespace: PowerNamespace,
				},
			}

			result, err := r.Reconcile(context.TODO(), req)

			if tc.expectError {
				assert.Error(t, err)
				if tc.errorContains != "" {
					assert.Contains(t, err.Error(), tc.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}

			// Check PowerNodeState exclusive pool entries.
			if len(tc.expectedContainers) > 0 {
				powerNodeState := &powerv1alpha1.PowerNodeState{}
				err = r.Client.Get(context.TODO(), client.ObjectKey{
					Name:      testNode + "-power-state",
					Namespace: PowerNamespace,
				}, powerNodeState)
				assert.NoError(t, err)

				// Collect all PowerContainers from the exclusive pools.
				var actualContainers []powerv1alpha1.PowerContainer
				if powerNodeState.Status.CPUPools == nil {
					t.Fatal("expected CPUPools to be set in PowerNodeState")
				}
				for _, exclusive := range powerNodeState.Status.CPUPools.Exclusive {
					actualContainers = append(actualContainers, exclusive.PowerContainers...)
				}

				require.Len(t, actualContainers, len(tc.expectedContainers))
				for _, expected := range tc.expectedContainers {
					var found bool
					for _, actual := range actualContainers {
						if actual.Name == expected.Name {
							found = true
							assert.Equal(t, expected.PowerProfile, actual.PowerProfile, "container %s profile mismatch", expected.Name)
							assert.ElementsMatch(t, expected.CPUIDs, actual.CPUIDs, "container %s CPUIDs mismatch", expected.Name)
							assert.ElementsMatch(t, expected.Errors, actual.Errors, "container %s errors mismatch", expected.Name)
							break
						}
					}
					assert.True(t, found, "expected container %s not found in PowerNodeState", expected.Name)
				}
			}

			// Verify result properties
			assert.Equal(t, time.Duration(0), result.RequeueAfter)
		})
	}
}

func TestPowerReleventPodPredicate(t *testing.T) {
	t.Setenv("NODE_NAME", "TestNode")

	makePod := func(ns, node string, initReqs,
		reqs map[corev1.ResourceName]resource.Quantity,
		withInit bool,
	) *corev1.Pod {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod",
				Namespace: ns,
			},
			Spec: corev1.PodSpec{
				NodeName: node,
				Containers: []corev1.Container{
					{
						Name: "c1",
						Resources: corev1.ResourceRequirements{
							Requests: reqs,
							Limits:   reqs,
						},
					},
				},
			},
		}
		if withInit {
			pod.Spec.InitContainers = []corev1.Container{
				{
					Name: "ic1",
					Resources: corev1.ResourceRequirements{
						Requests: initReqs,
						Limits:   initReqs,
					},
				},
			}
		}
		return pod
	}

	powerReq := map[corev1.ResourceName]resource.Quantity{
		corev1.ResourceName(ResourcePrefix + "performance"): *resource.NewQuantity(1, resource.DecimalSI),
	}
	cpuMemReq := map[corev1.ResourceName]resource.Quantity{
		corev1.ResourceCPU:    *resource.NewQuantity(1, resource.DecimalSI),
		corev1.ResourceMemory: *resource.NewQuantity(128, resource.DecimalSI),
	}

	cases := []struct {
		name string
		obj  client.Object
		want bool
	}{
		{
			name: "non-pod object returns false",
			obj:  &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n"}},
			want: false,
		},
		{
			name: "node mismatch returns false",
			obj:  makePod(PowerNamespace, "OtherNode", nil, powerReq, false),
			want: false,
		},
		{
			name: "no power requests returns false",
			obj:  makePod(PowerNamespace, "TestNode", nil, cpuMemReq, false),
			want: false,
		},
		{
			name: "container with power request returns true",
			obj:  makePod(PowerNamespace, "TestNode", nil, powerReq, false),
			want: true,
		},
		{
			name: "container with power request in other namespace returns true",
			obj:  makePod("test-namespace", "TestNode", nil, powerReq, true),
			want: true,
		},
		{
			name: "init container with power request returns true",
			obj:  makePod(PowerNamespace, "TestNode", powerReq, cpuMemReq, true),
			want: true,
		},
		{
			name: "multiple containers where one requests power returns true",
			obj: func() client.Object {
				pod := makePod(PowerNamespace, "TestNode", nil, cpuMemReq, false)
				pod.Spec.Containers = append(
					pod.Spec.Containers,
					corev1.Container{
						Name: "c2",
						Resources: corev1.ResourceRequirements{
							Requests: powerReq,
							Limits:   powerReq,
						},
					},
				)
				return pod
			}(),
			want: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := PowerReleventPodPredicate(tc.obj)
			if got != tc.want {
				t.Fatalf("PowerReleventPodPredicate() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestPowerPod_RestartRace_SharedPoolNotReady verifies that when the power node agent
// restarts, the PowerPod controller correctly requeues if CPUs are still in the reserved
// pool (shared pool not yet set up by PowerNodeConfig controller).
// This simulates the race: PowerPod reconciles before the shared pool is configured.
func TestPowerPod_RestartRace_SharedPoolNotReady(t *testing.T) {
	nodeName := "TestNode"
	t.Setenv("NODE_NAME", nodeName)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "restart-race-pod",
			Namespace: PowerNamespace,
			UID:       "restart-race-uid",
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
			Containers: []corev1.Container{
				{
					Name:      "test-container",
					Resources: defaultResources,
				},
			},
		},
		Status: corev1.PodStatus{
			Phase:    corev1.PodRunning,
			QOSClass: corev1.PodQOSGuaranteed,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:        "test-container",
					ContainerID: "containerd://test-cid-1",
				},
			},
		},
	}

	powerNodeState := &powerv1alpha1.PowerNodeState{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-power-state", nodeName),
			Namespace: PowerNamespace,
		},
	}

	fakePodResources := []*podresourcesapi.PodResources{
		{
			Name:      "restart-race-pod",
			Namespace: PowerNamespace,
			Containers: []*podresourcesapi.ContainerResources{
				{
					Name:   "test-container",
					CpuIds: []int64{10, 11, 12},
				},
			},
		},
	}

	objs := []runtime.Object{pod, defaultProfile, powerNodeState}
	podResourcesClient := createFakePodResourcesListerClient(fakePodResources)

	// Custom reconciler setup — we need to control the shared pool state
	// instead of using createPodReconcilerObject which gives CPUs 0-99.
	log.SetLogger(zap.New(
		zap.UseDevMode(true),
		func(opts *zap.Options) {
			opts.TimeEncoder = zapcore.ISO8601TimeEncoder
		},
	))
	s := scheme.Scheme
	_ = powerv1alpha1.AddToScheme(s)

	cl := fake.NewClientBuilder().
		WithRuntimeObjects(objs...).
		WithStatusSubresource(&powerv1alpha1.PowerNodeState{}).
		Build()

	state, _ := podstate.NewState()

	mockPowerLibrary := new(hostMock)
	mockPowerLibrary.On("GetExclusivePool", "performance").Return(createMockPoolWithCPUs([]uint{}))

	// Phase 1: Shared pool is EMPTY — simulates restart before shared pool is configured.
	// CPUs are still in the reserved pool.
	emptySharedPool := createMockPoolWithCPUs([]uint{})
	mockPowerLibrary.On("GetSharedPool").Return(emptySharedPool).Once()

	r := &PowerPodReconciler{
		Client:             cl,
		Log:                ctrl.Log.WithName("testing"),
		Scheme:             s,
		State:              state,
		PodResourcesClient: *podResourcesClient,
		PowerLibrary:       mockPowerLibrary,
	}

	req := reconcile.Request{
		NamespacedName: client.ObjectKey{
			Name:      pod.Name,
			Namespace: PowerNamespace,
		},
	}

	// First reconcile: CPUs not in shared pool → should requeue without error.
	result, err := r.Reconcile(context.TODO(), req)
	assert.NoError(t, err, "should not error, just requeue")
	assert.True(t, result.RequeueAfter > 0, "should requeue when CPUs not in shared pool")

	// Phase 2: PowerNodeConfig controller has now run — CPUs moved to shared pool.
	populatedSharedPool := createMockPoolWithCPUs([]uint{10, 11, 12, 13, 14, 15})
	mockPowerLibrary.On("GetSharedPool").Return(populatedSharedPool)

	// Second reconcile: CPUs are now in shared pool → should succeed without requeue.
	result, err = r.Reconcile(context.TODO(), req)
	assert.NoError(t, err, "should succeed after shared pool is populated")
	assert.Equal(t, result.RequeueAfter, time.Duration(0), "should not requeue when CPUs are in shared pool")
}

func TestPowerPod_areCPUsInSharedPool(t *testing.T) {
	tcases := []struct {
		name           string
		sharedPoolCPUs []uint
		cpuIDs         []uint
		expected       bool
	}{
		{
			name:           "all CPUs in shared pool",
			sharedPoolCPUs: []uint{0, 1, 2, 3, 4},
			cpuIDs:         []uint{1, 2, 3},
			expected:       true,
		},
		{
			name:           "some CPUs not in shared pool",
			sharedPoolCPUs: []uint{0, 1, 2},
			cpuIDs:         []uint{1, 2, 5},
			expected:       false,
		},
		{
			name:           "no CPUs in shared pool",
			sharedPoolCPUs: []uint{0, 1, 2},
			cpuIDs:         []uint{10, 11},
			expected:       false,
		},
		{
			name:           "empty cpuIDs returns true",
			sharedPoolCPUs: []uint{0, 1, 2},
			cpuIDs:         []uint{},
			expected:       true,
		},
		{
			name:           "empty shared pool returns false",
			sharedPoolCPUs: []uint{},
			cpuIDs:         []uint{1},
			expected:       false,
		},
	}

	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := new(hostMock)
			mockHost.On("GetSharedPool").Return(createMockPoolWithCPUs(tc.sharedPoolCPUs))

			r := &PowerPodReconciler{
				PowerLibrary: mockHost,
			}

			got := r.areCPUsInSharedPool(tc.cpuIDs)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestPowerPod_DetectCoresAdded(t *testing.T) {
	orig := []uint{1, 2, 3, 4}
	updated := []uint{1, 2, 4, 5}

	expectedResult := []uint{5}
	result := detectCoresAdded(orig, updated, &logr.Logger{})
	assert.ElementsMatch(t, result, expectedResult)
}

func TestPowerPod_generateCPUScalingOpts(t *testing.T) {
	minFreq := uint(1000000)
	maxFreq := uint(3700000)

	// Build a CPUList with mock CPUs 0, 1, 2. CPUs 0 and 1 are HT siblings.
	siblings := map[uint][]uint{0: {1}, 1: {0}, 2: nil}
	cpuList := make(power.CPUList, 3)
	for i, id := range []uint{0, 1, 2} {
		cpu := new(coreMock)
		cpu.On("GetID").Return(id)
		cpu.On("GetAbsMinMax").Return(minFreq, maxFreq)
		cpu.On("GetSiblings").Return(siblings[id])
		cpuList[i] = cpu
	}

	policy := &powerv1alpha1.CPUScalingPolicy{
		WorkloadType:               WorkloadTypePollingDPDK,
		SamplePeriod:               &metav1.Duration{Duration: 10 * time.Millisecond},
		CooldownPeriod:             &metav1.Duration{Duration: 30 * time.Millisecond},
		TargetUsage:                intPtr(80),
		AllowedUsageDifference:     intPtr(5),
		AllowedFrequencyDifference: intPtr(25),
		FallbackFreqPercent:        intPtr(50),
		AppFreqPercent:             intPtr(100),
		ScalePercentage:            intPtr(100),
	}

	tcases := []struct {
		name           string
		cpuIDs         []uint
		expectError    bool
		expectErrorIDs []string
		expectOptIDs   []uint
	}{
		{
			name:         "successfully generates scaling options for all CPUs",
			cpuIDs:       []uint{0, 2},
			expectOptIDs: []uint{0, 2},
		},
		{
			name:           "some CPUs not found in power library",
			cpuIDs:         []uint{0, 5, 9},
			expectError:    true,
			expectErrorIDs: []string{"5", "9"},
			expectOptIDs:   []uint{0},
		},
	}

	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			mockHost := new(hostMock)
			mockHost.On("GetAllCpus").Return(&cpuList)

			r := &PowerPodReconciler{PowerLibrary: mockHost}

			opts, err := r.generateCPUScalingOpts(policy, tc.cpuIDs)

			if tc.expectError {
				require.Error(t, err)
				for _, id := range tc.expectErrorIDs {
					assert.Contains(t, err.Error(), id)
				}
			} else {
				assert.NoError(t, err)
			}

			require.Len(t, opts, len(tc.expectOptIDs))
			actualIDs := make([]uint, len(opts))
			for i, o := range opts {
				actualIDs[i] = o.CPU.GetID()
			}
			assert.ElementsMatch(t, tc.expectOptIDs, actualIDs)

			// Verify scaling parameters on returned opts.
			for _, o := range opts {
				assert.Equal(t, 10*time.Millisecond, o.SamplePeriod)
				assert.Equal(t, 30*time.Millisecond, o.CooldownPeriod)
				assert.Equal(t, 80, o.TargetUsage)
				assert.Equal(t, 5, o.AllowedUsageDifference)
				assert.Equal(t, 25*1000, o.AllowedFrequencyDifference)
				assert.Equal(t, 1.0, o.ScaleFactor)
				assert.Equal(t, scaling.FrequencyNotYetSet, o.CurrentTargetFrequency)

				expectedFallback := minFreq + (maxFreq-minFreq)*50/100
				assert.Equal(t, int(expectedFallback), o.FallbackFreq)
				assert.Equal(t, int(maxFreq), o.AppFreq)
				assert.Equal(t, siblings[o.CPU.GetID()], o.SiblingCPUIDs)
			}
		})
	}
}

func TestPowerPod_Reconcile_WithCPUScalingPolicy(t *testing.T) {
	testNode := "TestNode"
	t.Setenv("NODE_NAME", testNode)

	testcases := []struct {
		name                                                    string
		profileName                                             string
		podUID                                                  string
		cpuIDs                                                  []uint
		sample, cooldown                                        time.Duration
		targetUsage, usageDiff, freqDiff, fallbackPct, scalePct int
	}{
		{
			name:        "profile1-two-cpus",
			profileName: "scaling-profile-1",
			podUID:      "pod-uid-foo",
			cpuIDs:      []uint{0, 1},
			sample:      10 * time.Millisecond,
			cooldown:    30 * time.Millisecond,
			targetUsage: 100, usageDiff: 10, freqDiff: 25, fallbackPct: 50, scalePct: 100,
		},
		{
			name:        "profile2-one-cpu",
			profileName: "scaling-profile-2",
			podUID:      "pod-uid-bar",
			cpuIDs:      []uint{5},
			sample:      100 * time.Millisecond,
			cooldown:    100 * time.Millisecond,
			targetUsage: 50, usageDiff: 5, freqDiff: 45, fallbackPct: 0, scalePct: 130,
		},
		{
			name:        "profile3-one-cpu",
			profileName: "scaling-profile-3",
			podUID:      "pod-uid-qux",
			cpuIDs:      []uint{7},
			sample:      300 * time.Millisecond,
			cooldown:    301 * time.Millisecond,
			targetUsage: 0, usageDiff: 0, freqDiff: 0, fallbackPct: 100, scalePct: 47,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// Build CPUScalingPolicy and PowerProfile.
			policy := &powerv1alpha1.CPUScalingPolicy{
				WorkloadType:               WorkloadTypePollingDPDK,
				SamplePeriod:               &metav1.Duration{Duration: tc.sample},
				CooldownPeriod:             &metav1.Duration{Duration: tc.cooldown},
				TargetUsage:                intPtr(tc.targetUsage),
				AllowedUsageDifference:     intPtr(tc.usageDiff),
				AllowedFrequencyDifference: intPtr(tc.freqDiff),
				FallbackFreqPercent:        intPtr(tc.fallbackPct),
				AppFreqPercent:             intPtr(100),
				ScalePercentage:            intPtr(tc.scalePct),
			}
			profile := &powerv1alpha1.PowerProfile{
				ObjectMeta: metav1.ObjectMeta{Name: tc.profileName, Namespace: PowerNamespace},
				Spec:       powerv1alpha1.PowerProfileSpec{CPUScalingPolicy: policy},
			}

			// Build resource requirements referencing the profile.
			profileResourceName := corev1.ResourceName(ResourcePrefix + tc.profileName)
			cpuCount := int64(len(tc.cpuIDs))
			resources := corev1.ResourceRequirements{
				Limits: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:  *resource.NewQuantity(cpuCount, resource.DecimalSI),
					"memory":            *resource.NewQuantity(200, resource.DecimalSI),
					profileResourceName: *resource.NewQuantity(cpuCount, resource.DecimalSI),
				},
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:  *resource.NewQuantity(cpuCount, resource.DecimalSI),
					"memory":            *resource.NewQuantity(200, resource.DecimalSI),
					profileResourceName: *resource.NewQuantity(cpuCount, resource.DecimalSI),
				},
			}

			// Build the pod.
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "dpdk-pod",
					Namespace: PowerNamespace,
					UID:       types.UID(tc.podUID),
				},
				Spec: corev1.PodSpec{
					NodeName: testNode,
					Containers: []corev1.Container{
						{Name: "dpdk-container", Resources: resources},
					},
					EphemeralContainers: []corev1.EphemeralContainer{},
				},
				Status: corev1.PodStatus{
					Phase:    corev1.PodRunning,
					QOSClass: corev1.PodQOSGuaranteed,
					ContainerStatuses: []corev1.ContainerStatus{
						{Name: "dpdk-container", ContainerID: "docker://abc123"},
					},
				},
			}

			// Build kubelet PodResources response.
			cpuIDs64 := make([]int64, len(tc.cpuIDs))
			for i, id := range tc.cpuIDs {
				cpuIDs64[i] = int64(id)
			}
			podResources := []*podresourcesapi.PodResources{
				{
					Name:      "dpdk-pod",
					Namespace: PowerNamespace,
					Containers: []*podresourcesapi.ContainerResources{
						{Name: "dpdk-container", CpuIds: cpuIDs64},
					},
				},
			}
			podResourcesClient := createFakePodResourcesListerClient(podResources)

			// Set up real power library with CPUs in the exclusive pool.
			host, teardown, err := fullDummySystem()
			assert.NoError(t, err)
			t.Cleanup(teardown)
			assert.NoError(t, host.GetSharedPool().SetCPUIDs(tc.cpuIDs))
			pool, err := host.AddExclusivePool(tc.profileName)
			assert.NoError(t, err)
			assert.NoError(t, pool.SetCPUIDs(tc.cpuIDs))

			// Create reconciler and override with real power library and DPDK mocks.
			r, err := createPodReconcilerObject(
				[]runtime.Object{
					profile, pod, defaultPowerNodeState,
					&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: testNode}},
				},
				podResourcesClient,
			)
			assert.NoError(t, err)
			r.PowerLibrary = host

			dpdkmk := new(DPDKTelemetryClientMock)
			dpdkmk.On("EnsureConnection", &scaling.DPDKTelemetryConnectionData{
				PodUID:      tc.podUID,
				WatchedCPUs: tc.cpuIDs,
			}).Return().Once()
			r.DPDKTelemetryClient = dpdkmk

			scalingMgrMock := new(ScalingMgrMock)
			scalingMgrMock.On("AddCPUScaling", mock.Anything).Return().Once()
			r.CPUScalingManager = scalingMgrMock

			// Reconcile the pod.
			_, err = r.Reconcile(context.TODO(), reconcile.Request{
				NamespacedName: client.ObjectKey{Name: "dpdk-pod", Namespace: PowerNamespace},
			})
			assert.NoError(t, err)

			// Verify DPDK connection was created.
			dpdkmk.AssertExpectations(t)

			// Verify AddCPUScaling was called with correct scaling options.
			scalingMgrMock.AssertExpectations(t)
			if assert.Equal(t, 1, len(scalingMgrMock.Calls)) {
				actualScalingOpts := scalingMgrMock.Calls[0].Arguments.Get(0).([]scaling.CPUScalingOpts)
				actualIDs := make([]uint, 0, len(actualScalingOpts))
				for _, o := range actualScalingOpts {
					actualIDs = append(actualIDs, o.CPU.GetID())
				}
				assert.ElementsMatch(t, tc.cpuIDs, actualIDs)
				assert.Equal(t, tc.sample, actualScalingOpts[0].SamplePeriod)
				assert.Equal(t, tc.cooldown, actualScalingOpts[0].CooldownPeriod)
				assert.Equal(t, tc.targetUsage, actualScalingOpts[0].TargetUsage)
				assert.Equal(t, tc.usageDiff, actualScalingOpts[0].AllowedUsageDifference)
				assert.Equal(t, tc.freqDiff*1000, actualScalingOpts[0].AllowedFrequencyDifference)
				assert.Equal(t, float64(tc.scalePct)/100, actualScalingOpts[0].ScaleFactor)

				cpu := host.GetAllCpus().ByID(tc.cpuIDs[0])
				minFreq, maxFreq := cpu.GetAbsMinMax()
				fallbackFreq := minFreq + (maxFreq-minFreq)*(uint(tc.fallbackPct))/100
				assert.Equal(t, int(fallbackFreq), actualScalingOpts[0].FallbackFreq)
			}
		})
	}
}

func TestPowerPod_Reconcile_MultipleDPDKContainersRejected(t *testing.T) {
	testNode := "TestNode"
	t.Setenv("NODE_NAME", testNode)

	profileName := "dpdk-profile"

	policy := &powerv1alpha1.CPUScalingPolicy{
		WorkloadType:               WorkloadTypePollingDPDK,
		SamplePeriod:               &metav1.Duration{Duration: 10 * time.Millisecond},
		CooldownPeriod:             &metav1.Duration{Duration: 30 * time.Millisecond},
		TargetUsage:                intPtr(80),
		AllowedUsageDifference:     intPtr(5),
		AllowedFrequencyDifference: intPtr(25),
		FallbackFreqPercent:        intPtr(50),
		AppFreqPercent:             intPtr(100),
		ScalePercentage:            intPtr(100),
	}
	profile := &powerv1alpha1.PowerProfile{
		ObjectMeta: metav1.ObjectMeta{Name: profileName, Namespace: PowerNamespace},
		Spec:       powerv1alpha1.PowerProfileSpec{CPUScalingPolicy: policy},
	}

	// Both containers request the same DPDK profile.
	profileResourceName := corev1.ResourceName(ResourcePrefix + profileName)
	makeResources := func(cpuCount int64) corev1.ResourceRequirements {
		return corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:  *resource.NewQuantity(cpuCount, resource.DecimalSI),
				"memory":            *resource.NewQuantity(200, resource.DecimalSI),
				profileResourceName: *resource.NewQuantity(cpuCount, resource.DecimalSI),
			},
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:  *resource.NewQuantity(cpuCount, resource.DecimalSI),
				"memory":            *resource.NewQuantity(200, resource.DecimalSI),
				profileResourceName: *resource.NewQuantity(cpuCount, resource.DecimalSI),
			},
		}
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "multi-dpdk-pod",
			Namespace: PowerNamespace,
			UID:       "multi-dpdk-uid",
		},
		Spec: corev1.PodSpec{
			NodeName: testNode,
			Containers: []corev1.Container{
				{Name: "dpdk-container-1", Resources: makeResources(2)},
				{Name: "dpdk-container-2", Resources: makeResources(2)},
			},
			EphemeralContainers: []corev1.EphemeralContainer{},
		},
		Status: corev1.PodStatus{
			Phase:    corev1.PodRunning,
			QOSClass: corev1.PodQOSGuaranteed,
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "dpdk-container-1", ContainerID: "docker://c1"},
				{Name: "dpdk-container-2", ContainerID: "docker://c2"},
			},
		},
	}

	podResourcesClient := createFakePodResourcesListerClient([]*podresourcesapi.PodResources{{
		Name:      "multi-dpdk-pod",
		Namespace: PowerNamespace,
		Containers: []*podresourcesapi.ContainerResources{
			{Name: "dpdk-container-1", CpuIds: []int64{0, 1}},
			{Name: "dpdk-container-2", CpuIds: []int64{2, 3}},
		},
	}})

	// Set up real power library with CPUs in the exclusive pool.
	host, teardown, err := fullDummySystem()
	assert.NoError(t, err)
	t.Cleanup(teardown)
	assert.NoError(t, host.GetSharedPool().SetCPUIDs([]uint{0, 1, 2, 3}))
	pool, err := host.AddExclusivePool(profileName)
	assert.NoError(t, err)
	assert.NoError(t, pool.SetCPUIDs([]uint{0, 1, 2, 3}))

	r, err := createPodReconcilerObject(
		[]runtime.Object{
			profile, pod, defaultPowerNodeState,
			&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: testNode}},
		},
		podResourcesClient,
	)
	assert.NoError(t, err)
	r.PowerLibrary = host

	// Only one EnsureConnection and one AddCPUScaling call expected (for the first container).
	dpdkmk := new(DPDKTelemetryClientMock)
	dpdkmk.On("EnsureConnection", &scaling.DPDKTelemetryConnectionData{
		PodUID:      "multi-dpdk-uid",
		WatchedCPUs: []uint{0, 1},
	}).Return().Once()
	r.DPDKTelemetryClient = dpdkmk

	scalingMgrMock := new(ScalingMgrMock)
	scalingMgrMock.On("AddCPUScaling", mock.MatchedBy(func(opts []scaling.CPUScalingOpts) bool {
		// Only the first container's CPUs (0, 1) should be scaled.
		if len(opts) != 2 {
			return false
		}
		ids := []uint{opts[0].CPU.GetID(), opts[1].CPU.GetID()}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		return reflect.DeepEqual(ids, []uint{0, 1})
	})).Return().Once()
	r.CPUScalingManager = scalingMgrMock

	_, err = r.Reconcile(context.TODO(), reconcile.Request{
		NamespacedName: client.ObjectKey{Name: "multi-dpdk-pod", Namespace: PowerNamespace},
	})
	assert.NoError(t, err)

	dpdkmk.AssertExpectations(t)
	scalingMgrMock.AssertExpectations(t)

	// Verify the second container has an error in PowerNodeState.
	pns := &powerv1alpha1.PowerNodeState{}
	err = r.Client.Get(context.TODO(), client.ObjectKey{
		Name:      testNode + "-power-state",
		Namespace: PowerNamespace,
	}, pns)
	assert.NoError(t, err)

	require.NotNil(t, pns.Status.CPUPools)
	require.Len(t, pns.Status.CPUPools.Exclusive, 1)
	containers := pns.Status.CPUPools.Exclusive[0].PowerContainers
	require.Len(t, containers, 2)

	// Find the second container and verify it has the expected error.
	for _, pc := range containers {
		if pc.Name == "dpdk-container-2" {
			require.Len(t, pc.Errors, 1)
			assert.Contains(t, pc.Errors[0], "DPDK dynamic frequency scaling is only supported for a single container per pod")
		} else {
			assert.Empty(t, pc.Errors, "first DPDK container should have no errors")
		}
	}
}
