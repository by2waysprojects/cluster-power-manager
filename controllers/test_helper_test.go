package controllers

import (
	// "errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"context"

	"github.com/cluster-power-manager/cluster-power-manager/internal/power"
	"github.com/cluster-power-manager/cluster-power-manager/internal/scaling"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/mock"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

type hostMock struct {
	mock.Mock
	power.Host
}

func (m *hostMock) Topology() power.Topology {
	return m.Called().Get(0).(power.Topology)
}

func (m *hostMock) GetAllExclusivePools() *power.PoolList {
	return m.Called().Get(0).(*power.PoolList)
}

func (m *hostMock) SetName(name string) {
	m.Called(name)
}

func (m *hostMock) GetName() string {
	return m.Called().String(0)
}

func (m *hostMock) GetFreqRanges() power.CoreTypeList {
	return m.Called().Get(0).(power.CoreTypeList)
}

func (m *hostMock) GetFeaturesInfo() power.FeatureSet {
	ret := m.Called().Get(0)
	if ret == nil {
		return nil
	} else {
		return ret.(power.FeatureSet)
	}
}

func (m *hostMock) GetReservedPool() power.Pool {
	ret := m.Called().Get(0)
	if ret == nil {
		return nil
	} else {
		return ret.(power.Pool)
	}
}

func (m *hostMock) GetSharedPool() power.Pool {
	ret := m.Called().Get(0)
	if ret == nil {
		return nil
	} else {
		return ret.(power.Pool)
	}
}
func (m *hostMock) AddExclusivePool(poolName string) (power.Pool, error) {
	args := m.Called(poolName)
	retPool := args.Get(0)
	if retPool == nil {
		return nil, args.Error(1)
	} else {
		return retPool.(power.Pool), args.Error(1)
	}
}

func (m *hostMock) GetExclusivePool(poolName string) power.Pool {
	ret := m.Called(poolName).Get(0)
	if ret == nil {
		return nil
	} else {
		return ret.(power.Pool)
	}
}

func (m *hostMock) GetAllCpus() *power.CPUList {
	ret := m.Called().Get(0)
	if ret == nil {
		return nil
	} else {
		return ret.(*power.CPUList)
	}
}

type poolMock struct {
	mock.Mock
	power.Pool
}

// createMockPoolWithCPUs creates a poolMock that returns the given CPU IDs
// and sets up MoveCPUIDs to succeed by default
func createMockPoolWithCPUs(cpuIDs []uint) *poolMock {
	pm := new(poolMock)
	// Create a CPUList with mock Cpus that return the given IDs
	cpuList := make(power.CPUList, len(cpuIDs))
	for i, id := range cpuIDs {
		cpu := new(coreMock)
		cpu.On("GetID").Return(id)
		cpuList[i] = cpu
	}
	pm.On("Cpus").Return(&cpuList)
	pm.On("MoveCPUIDs", mock.Anything).Return(nil)
	pm.On("GetPowerProfile").Return(nil)
	return pm
}

func (m *poolMock) Name() string {
	return m.Called().String(0)
}

func (m *poolMock) Cpus() *power.CPUList {
	args := m.Called().Get(0)
	if args == nil {
		return nil
	}
	return args.(*power.CPUList)
}

func (m *poolMock) SetCPUIDs(cpuIDs []uint) error {
	return m.Called(cpuIDs).Error(0)
}

func (m *poolMock) Remove() error {
	return m.Called().Error(0)
}

func (m *poolMock) MoveCPUIDs(coreIDs []uint) error {
	return m.Called(coreIDs).Error(0)
}

func (m *poolMock) MoveCpus(cores power.CPUList) error {
	return m.Called(cores).Error(0)
}

func (m *poolMock) SetPowerProfile(profile power.Profile) error {
	args := m.Called(profile)
	return args.Error(0)
}

func (m *poolMock) GetPowerProfile() power.Profile {
	args := m.Called().Get(0)
	if args == nil {
		return nil
	}
	return args.(power.Profile)
}

type profMock struct {
	mock.Mock
	power.Profile
}

func (m *profMock) Name() string {
	return m.Called().String(0)
}

func (m *profMock) Epp() string {
	return m.Called().String(0)
}

func (m *profMock) MaxFreq() uint {
	return uint(m.Called().Int(0))
}

func (m *profMock) MinFreq() uint {
	return uint(m.Called().Int(0))
}

func (m *profMock) Governor() string {
	return m.Called().String(0)
}

type coreMock struct {
	mock.Mock
	power.CPU
}

func (m *coreMock) GetID() uint {
	args := m.Called()
	return args.Get(0).(uint)
}

func (m *coreMock) GetAbsMinMax() (uint, uint) {
	args := m.Called()
	return args.Get(0).(uint), args.Get(1).(uint)
}

func (m *coreMock) GetSiblings() []uint {
	args := m.Called()
	return args.Get(0).([]uint)
}

type mockCPUTopology struct {
	mock.Mock
	power.Topology
}

func (m *mockCPUTopology) getID() uint {
	return m.Called().Get(0).(uint)
}

func (m *mockCPUTopology) SetUncore(uncore power.Uncore) error {
	return m.Called(uncore).Error(0)
}

func (m *mockCPUTopology) applyUncore() error {
	return m.Called().Error(0)
}

func (m *mockCPUTopology) getEffectiveUncore() power.Uncore {
	ret := m.Called()
	if ret.Get(0) != nil {
		return ret.Get(0).(power.Uncore)
	}
	return nil
}

func (m *mockCPUTopology) addCPU(u uint) (power.CPU, error) {
	ret := m.Called(u)

	var r0 power.CPU
	var r1 error

	if ret.Get(0) != nil {
		r0 = ret.Get(0).(power.CPU)
	}
	r1 = ret.Error(1)

	return r0, r1
}

func (m *mockCPUTopology) CPUs() *power.CPUList {
	ret := m.Called()

	var r0 *power.CPUList
	if ret.Get(0) != nil {
		r0 = ret.Get(0).(*power.CPUList)
	}

	return r0
}

func (m *mockCPUTopology) Packages() *[]power.Package {
	ret := m.Called()

	var r0 *[]power.Package
	if ret.Get(0) != nil {
		r0 = ret.Get(0).(*[]power.Package)

	}
	return r0
}

func (m *mockCPUTopology) Package(id uint) power.Package {
	ret := m.Called(id)

	var r0 power.Package
	if ret.Get(0) != nil {
		r0 = ret.Get(0).(power.Package)
	}

	return r0
}

type mockCPUPackage struct {
	mock.Mock
	power.Package
}
type mockPackageList struct {
	mock.Mock
}

func (m *mockCPUPackage) MakeList() []power.Package {
	return []power.Package{m}
}
func (m *mockCPUPackage) getID() uint {
	return m.Called().Get(0).(uint)
}

func (m *mockCPUPackage) SetUncore(uncore power.Uncore) error {
	return m.Called(uncore).Error(0)
}

func (m *mockCPUPackage) applyUncore() error {
	return m.Called().Error(0)
}

func (m *mockCPUPackage) getEffectiveUncore() power.Uncore {
	ret := m.Called()
	if ret.Get(0) != nil {
		return ret.Get(0).(power.Uncore)
	}
	return nil
}

func (m *mockCPUPackage) addCPU(u uint) (power.CPU, error) {
	ret := m.Called(u)

	var r0 power.CPU
	var r1 error

	if ret.Get(0) != nil {
		r0 = ret.Get(0).(power.CPU)
	}
	r1 = ret.Error(1)

	return r0, r1
}

func (m *mockCPUPackage) CPUs() *power.CPUList {
	ret := m.Called()

	var r0 *power.CPUList
	if ret.Get(0) != nil {
		r0 = ret.Get(0).(*power.CPUList)
	}

	return r0
}

func (m *mockCPUPackage) Dies() *[]power.Die {
	ret := m.Called()

	var r0 *[]power.Die
	if ret.Get(0) != nil {
		r0 = ret.Get(0).(*[]power.Die)

	}
	return r0
}

func (m *mockCPUPackage) Die(id uint) power.Die {
	ret := m.Called(id)

	var r0 power.Die
	if ret.Get(0) != nil {
		r0 = ret.Get(0).(power.Die)
	}

	return r0
}

type mockCPUDie struct {
	mock.Mock
	power.Die
}

func (m *mockCPUDie) MakeList() []power.Die {
	return []power.Die{m}
}

func (m *mockCPUDie) getID() uint {
	return m.Called().Get(0).(uint)
}

func (m *mockCPUDie) SetUncore(uncore power.Uncore) error {
	return m.Called(uncore).Error(0)
}

func (m *mockCPUDie) applyUncore() error {
	return m.Called().Error(0)
}

func (m *mockCPUDie) getEffectiveUncore() power.Uncore {
	ret := m.Called()
	if ret.Get(0) != nil {
		return ret.Get(0).(power.Uncore)
	}
	return nil
}

func (m *mockCPUDie) addCPU(u uint) (power.CPU, error) {
	ret := m.Called(u)

	var r0 power.CPU
	var r1 error

	if ret.Get(0) != nil {
		r0 = ret.Get(0).(power.CPU)
	}
	r1 = ret.Error(1)

	return r0, r1
}

func (m *mockCPUDie) CPUs() *power.CPUList {
	ret := m.Called()

	var r0 *power.CPUList
	if ret.Get(0) != nil {
		r0 = ret.Get(0).(*power.CPUList)
	}

	return r0
}

func (m *mockCPUDie) Cores() *[]power.Core {
	ret := m.Called()

	var r0 *[]power.Core
	if ret.Get(0) != nil {
		r0 = ret.Get(0).(*[]power.Core)

	}
	return r0
}

func (m *mockCPUDie) Core(id uint) power.Core {
	ret := m.Called(id)

	var r0 power.Core
	if ret.Get(0) != nil {
		r0 = ret.Get(0).(power.Core)
	}

	return r0
}

type frequencySetMock struct {
	mock.Mock
	power.CPUFrequencySet
}

func (m *frequencySetMock) GetMax() uint {
	return m.Called().Get(0).(uint)
}

func (m *frequencySetMock) GetMin() uint {
	return m.Called().Get(0).(uint)
}

// ScalingManager mock
type ScalingMgrMock struct {
	scaling.CPUScalingManager
	mock.Mock
}

func (m *ScalingMgrMock) AddCPUScaling(configs []scaling.CPUScalingOpts) {
	m.Called(configs)
}

func (m *ScalingMgrMock) RemoveCPUScaling(cpuIDs []uint) {
	m.Called(cpuIDs)
}

// Satisfy manager.Runnable
func (m *ScalingMgrMock) Start(ctx context.Context) error { return nil }

// DPDKTelemetryClient mock
type DPDKTelemetryClientMock struct {
	scaling.DPDKTelemetryClient
	mock.Mock
}

func (cl *DPDKTelemetryClientMock) EnsureConnection(data *scaling.DPDKTelemetryConnectionData) {
	cl.Called(data)
}

func (cl *DPDKTelemetryClientMock) ListConnections() []scaling.DPDKTelemetryConnectionData {
	args := cl.Called()
	return args.Get(0).([]scaling.DPDKTelemetryConnectionData)
}

func (cl *DPDKTelemetryClientMock) CloseConnection(podUID string) {
	cl.Called(podUID)
}

func (cl *DPDKTelemetryClientMock) GetUsagePercent(cpuID uint) (int, error) {
	args := cl.Called(cpuID)
	return args.Int(0), args.Error(1)
}

func (cl *DPDKTelemetryClientMock) Close() { cl.Called() }

func intPtr(v int) *int { return &v }

func setupDummyFiles(cores int, packages int, diesPerPackage int, cpufiles map[string]string) (power.Host, func(), error) {
	// variables for various files
	path := "testing/cpus"
	pStatesDrvFile := "cpufreq/scaling_driver"

	cpuMaxFreqFile := "cpufreq/cpuinfo_max_freq"
	cpuMinFreqFile := "cpufreq/cpuinfo_min_freq"
	scalingMaxFile := "cpufreq/scaling_max_freq"
	scalingMinFile := "cpufreq/scaling_min_freq"
	scalingGovFile := "cpufreq/scaling_governor"
	availGovFile := "cpufreq/scaling_available_governors"
	eppFile := "cpufreq/energy_performance_preference"
	cpuTopologyDir := "topology/"
	packageIDFile := cpuTopologyDir + "physical_package_id"
	dieIDFile := cpuTopologyDir + "die_id"
	coreIDFile := cpuTopologyDir + "core_id"
	uncoreDir := path + "/intel_uncore_frequency/"
	uncoreInitMaxFreqFile := "initial_max_freq_khz"
	uncoreInitMinFreqFile := "initial_min_freq_khz"
	uncoreMaxFreqFile := "max_freq_khz"
	uncoreMinFreqFile := "min_freq_khz"
	cstates := map[int]map[string]string{
		0: {"name": "C0", "latency": "0", "default_status": "enabled"},
		1: {"name": "C1", "latency": "1", "default_status": "enabled"},
		2: {"name": "C1E", "latency": "10", "default_status": "enabled"},
		3: {"name": "C3", "latency": "100", "default_status": "enabled"},
	}

	// if we're setting uncore we need to spoof the module being loaded
	_, ok := cpufiles["uncore_max"]
	if ok {
		os.Mkdir("testing", os.ModePerm)
		os.WriteFile("testing/proc.modules", []byte("intel_uncore_frequency"+"\n"), 0o644)
		os.MkdirAll(filepath.Join(uncoreDir, "package_00_die_00"), os.ModePerm)
	}
	die := 0
	pkg := 0
	var strPkg, strDie string
	pkgDir := "package_00_die_00/"
	increment := diesPerPackage * packages
	coresPerDie := cores / increment
	for i := 0; i < cores; i++ {
		cpuName := "cpu" + fmt.Sprint(i)
		cpudir := filepath.Join(path, cpuName)
		os.MkdirAll(filepath.Join(cpudir, "cpufreq"), os.ModePerm)
		os.MkdirAll(filepath.Join(cpudir, "topology"), os.ModePerm)
		// used to divide cores between packages and dies
		if i%coresPerDie == 0 && i != 0 && packages != 0 {
			if die == diesPerPackage-1 && pkg != (packages-1) {
				die = 0
				pkg++
			} else if die != (diesPerPackage - 1) {
				die++
			}

			if pkg > 10 {
				strPkg = fmt.Sprint(pkg)
			} else {
				strPkg = "0" + fmt.Sprint(pkg)
			}
			if die > 10 {
				strDie = fmt.Sprint(die)
			} else {
				strDie = "0" + fmt.Sprint(die)
			}
			pkgDir = "package_" + strPkg + "_die_" + strDie + "/"
			os.MkdirAll(filepath.Join(uncoreDir, pkgDir), os.ModePerm)
		}
		if packages != 0 {
			os.WriteFile(filepath.Join(cpudir, packageIDFile), []byte(fmt.Sprint(pkg)+"\n"), 0o664)
			os.WriteFile(filepath.Join(cpudir, dieIDFile), []byte(fmt.Sprint(die)+"\n"), 0o664)
			os.WriteFile(filepath.Join(cpudir, coreIDFile), []byte(fmt.Sprint(i)+"\n"), 0o664)
		}
		for prop, value := range cpufiles {
			switch prop {
			case "driver":
				os.WriteFile(filepath.Join(cpudir, pStatesDrvFile), []byte(value+"\n"), 0o664)
			case "max":
				os.WriteFile(filepath.Join(cpudir, scalingMaxFile), []byte(value+"\n"), 0o644)
				os.WriteFile(filepath.Join(cpudir, cpuMaxFreqFile), []byte(value+"\n"), 0o644)
			case "min":
				os.WriteFile(filepath.Join(cpudir, scalingMinFile), []byte(value+"\n"), 0o644)
				os.WriteFile(filepath.Join(cpudir, cpuMinFreqFile), []byte(value+"\n"), 0o644)
			case "epp":
				os.WriteFile(filepath.Join(cpudir, eppFile), []byte(value+"\n"), 0o644)
			case "governor":
				os.WriteFile(filepath.Join(cpudir, scalingGovFile), []byte(value+"\n"), 0o644)
			case "available_governors":
				os.WriteFile(filepath.Join(cpudir, availGovFile), []byte(value+"\n"), 0o644)
			case "uncore_max":
				os.WriteFile(filepath.Join(uncoreDir, pkgDir, uncoreInitMaxFreqFile), []byte(value+"\n"), 0o644)
				os.WriteFile(filepath.Join(uncoreDir, pkgDir, uncoreMaxFreqFile), []byte(value+"\n"), 0o644)
			case "uncore_min":
				os.WriteFile(filepath.Join(uncoreDir, pkgDir, uncoreInitMinFreqFile), []byte(value+"\n"), 0o644)
				os.WriteFile(filepath.Join(uncoreDir, pkgDir, uncoreMinFreqFile), []byte(value+"\n"), 0o644)
			case "cstates":
				for i, stateInfo := range cstates {
					statedir := "cpuidle/state" + fmt.Sprint(i)
					os.MkdirAll(filepath.Join(cpudir, statedir), os.ModePerm)
					os.MkdirAll(filepath.Join(path, "cpuidle"), os.ModePerm)
					os.WriteFile(filepath.Join(path, "cpuidle", "current_driver"), []byte(value+"\n"), 0o644)
					os.WriteFile(filepath.Join(cpudir, statedir, "name"), []byte(stateInfo["name"]+"\n"), 0o644)
					os.WriteFile(filepath.Join(cpudir, statedir, "disable"), []byte("0\n"), 0o644)
					os.WriteFile(filepath.Join(cpudir, statedir, "latency"), []byte(stateInfo["latency"]+"\n"), 0o644)
					os.WriteFile(filepath.Join(cpudir, statedir, "default_status"), []byte(stateInfo["default_status"]+"\n"), 0o644)
				}
			}

		}
	}

	restoreHostIdentity := power.PinTestHostIdentity()
	host, err := power.CreateInstanceWithConf("test-node", power.LibConfig{CPUPath: "testing/cpus", ModulePath: "testing/proc.modules", Cores: uint(cores)})
	return host, func() {
		os.RemoveAll(strings.Split(path, "/")[0])
		restoreHostIdentity()
	}, err
}

// default dummy file system to be used in standard tests
func fullDummySystem() (power.Host, func(), error) {
	return setupDummyFiles(86, 1, 2, map[string]string{
		"driver": "intel_pstate", "max": "3700000", "min": "1000000",
		"epp": "performance", "governor": "performance",
		"available_governors": "powersave performance",
		"uncore_max":          "2400000", "uncore_min": "1200000",
		"cstates": "intel_idle"})
}

// mock required for testing setupwithmanager
type clientMock struct {
	mock.Mock
	client.Client
}

// mock required for testing client errs
type errClient struct {
	client.Client
	mock.Mock
}

func (e *errClient) Get(ctx context.Context, namespacedName types.NamespacedName, obj client.Object, opts ...client.GetOption) error {
	if len(opts) != 0 {
		return e.Called(ctx, namespacedName, obj, opts).Error(0)
	}
	return e.Called(ctx, namespacedName, obj).Error(0)
}
func (e *errClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if len(opts) != 0 {
		return e.Called(ctx, list, opts).Error(0)

	}
	return e.Called(ctx, list).Error(0)
}

func (e *errClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if len(opts) != 0 {
		return e.Called(ctx, obj, opts).Error(0)
	}
	return e.Called(ctx, obj).Error(0)
}

func (e *errClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if len(opts) != 0 {
		return e.Called(ctx, obj, opts).Error(0)
	}
	return e.Called(ctx, obj).Error(0)
}

func (e *errClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if len(opts) != 0 {
		return e.Called(ctx, obj, opts).Error(0)
	}
	return e.Called(ctx, obj).Error(0)
}

func (e *errClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	if len(opts) != 0 {
		return e.Called(ctx, obj, opts).Error(0)
	}
	return e.Called(ctx, obj).Error(0)
}

func (e *errClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if len(opts) != 0 {
		return e.Called(ctx, obj, patch, opts).Error(0)
	}
	return e.Called(ctx, obj, patch).Error(0)
}

func (e *errClient) Status() client.SubResourceWriter {
	return e.Called().Get(0).(client.SubResourceWriter)
}

type mockResourceWriter struct {
	mock.Mock
	client.SubResourceWriter
}

func (m *mockResourceWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	if len(opts) != 0 {
		return m.Called(ctx, obj, opts).Error(0)
	}
	return m.Called(ctx, obj).Error(0)
}

func (m *mockResourceWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if len(opts) != 0 {
		return m.Called(ctx, obj, patch, opts).Error(0)
	}
	return m.Called(ctx, obj, patch).Error(0)
}

type mgrMock struct {
	mock.Mock
	manager.Manager
}

func (m *mgrMock) Add(r manager.Runnable) error {
	return m.Called(r).Error(0)
}

func (m *mgrMock) Elected() <-chan struct{} {
	return m.Called().Get(0).(<-chan struct{})
}

func (m *mgrMock) AddMetricsExtraHandler(path string, handler http.Handler) error {
	return m.Called(path, handler).Get(0).(error)
}

func (m *mgrMock) AddHealthzCheck(name string, check healthz.Checker) error {
	return m.Called(name, check).Get(0).(error)
}

func (m *mgrMock) AddReadyzCheck(name string, check healthz.Checker) error {
	return m.Called(name, check).Get(0).(error)
}

func (m *mgrMock) Start(ctx context.Context) error {
	return m.Called(ctx).Get(0).(error)
}

func (m *mgrMock) GetWebhookServer() webhook.Server {
	return m.Called().Get(0).(webhook.Server)
}

func (m *mgrMock) GetLogger() logr.Logger {
	return m.Called().Get(0).(logr.Logger)

}

func (m *mgrMock) GetControllerOptions() config.Controller {
	return m.Called().Get(0).(config.Controller)
}

func (m *mgrMock) GetScheme() *runtime.Scheme {
	return m.Called().Get(0).(*runtime.Scheme)
}
func (m *mgrMock) SetFields(i interface{}) error {
	return m.Called(i).Error(0)
}

func (m *mgrMock) GetCache() cache.Cache {
	return m.Called().Get(0).(cache.Cache)
}

func (m *mgrMock) GetFieldIndexer() client.FieldIndexer {
	return m.Called().Get(0).(client.FieldIndexer)
}

type fieldIndexerMock struct {
	client.FieldIndexer
}

func (f *fieldIndexerMock) IndexField(_ context.Context, _ client.Object, _ string, _ client.IndexerFunc) error {
	return nil
}

type cacheMk struct {
	cache.Cache
	mock.Mock
}
