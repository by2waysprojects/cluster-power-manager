package scaling

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap/zapcore"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

type MockConn struct {
	mock.Mock
	net.Conn
}

func (mc *MockConn) SetWriteDeadline(t time.Time) error {
	return mc.Called(t).Error(0)
}

func (mc *MockConn) Write(c []byte) (int, error) {
	args := mc.Called(c)
	return args.Int(0), args.Error(1)
}

func (mc *MockConn) SetReadDeadline(t time.Time) error {
	return mc.Called(t).Error(0)
}

func (mc *MockConn) Read(buf []byte) (int, error) {
	args := mc.Called(buf)
	copy(buf, args.Get(2).([]byte))
	return args.Int(0), args.Error(1)
}

func (mc *MockConn) Close() error {
	return mc.Called().Error(0)
}

type metricsMap map[uint]telemetryResult

var (
	usageOutputString = `{
		"/eal/lcore/usage": {
			"lcore_ids": [1, 2, 3],
			"total_cycles": [2142696834760, 2142696698860, 2142696553160],
			"busy_cycles": [1704128133420, 1704896313700, 1714666267580]
		}
	}`
)

func createNewDPDKTelemetryClient() dpdkTelemetryClientImpl {
	log.SetLogger(zap.New(
		zap.UseDevMode(true),
		func(opts *zap.Options) {
			opts.TimeEncoder = zapcore.ISO8601TimeEncoder
		},
	))

	return dpdkTelemetryClientImpl{
		log: ctrl.Log.WithName("test-log"),
	}
}

func TestDPDKTelemetryClient_EnsureConnection(t *testing.T) {
	expectedUID := "foo"
	usedUID := ""
	expectedCPUList := []uint{0, 1, 2}
	usedCPUList := []uint{}

	t.Cleanup(func() {
		testHookNewSocketConnection = nil
	})
	testHookNewSocketConnection = func(conn *dpdkTelemetryConnection) {
		usedUID = conn.podUID
		usedCPUList = conn.watchedCPUs
	}
	newConnData := &DPDKTelemetryConnectionData{
		PodUID:      expectedUID,
		WatchedCPUs: expectedCPUList,
	}

	cl := createNewDPDKTelemetryClient()

	cl.EnsureConnection(newConnData)
	assert.Equal(t, expectedUID, usedUID)
	assert.Equal(t, expectedCPUList, usedCPUList)
}

func TestDPDKTelemetryClient_ListConnections(t *testing.T) {
	expectedUID := "foo"
	expectedCPUList := []uint{0, 1, 2, 3}
	dummyConn := &dpdkTelemetryConnection{
		podUID: expectedUID,
	}
	dummyConn.watchedCPUs = expectedCPUList

	cl := createNewDPDKTelemetryClient()
	cl.connections.Store(expectedUID, dummyConn)

	connData := cl.ListConnections()

	assert.NotEmpty(t, connData)
	assert.Equal(t, expectedUID, connData[0].PodUID)
	assert.Equal(t, expectedCPUList, connData[0].WatchedCPUs)
}

func TestDPDKTelemetryClient_CloseConnection(t *testing.T) {
	uid := "foo"
	closeCallled := false
	t.Cleanup(func() {
		testHookCloseConnection = nil
	})
	testHookCloseConnection = func() {
		closeCallled = true
	}
	dummyConn := &dpdkTelemetryConnection{}

	cl := createNewDPDKTelemetryClient()

	cl.connections.Store(uid, dummyConn)

	cl.CloseConnection(uid)
	assert.True(t, closeCallled)
	cl.connections.Range(func(key, value any) bool {
		t.Error("Socket map was not cleaned.")
		return false
	})
}

func TestDPDKTelemetryClient_GetUsagePercent(t *testing.T) {
	tcases := []struct {
		testCase    string
		cpuID       uint
		usageResult *telemetryResult
		evalFn      func(v int, e error) bool
	}{
		{
			testCase: "Test Case 1 - Metrics Missing",
			cpuID:    0,
			evalFn: func(v int, e error) bool {
				return assert.ErrorIs(t, e, ErrDPDKMetricMissing)
			},
		},
		{
			testCase:    "Test Case 2 - Results with an error",
			cpuID:       3,
			usageResult: &telemetryResult{0, ErrDPDKMetricNotProvided},
			evalFn: func(v int, e error) bool {
				return assert.ErrorIs(t, e, ErrDPDKMetricNotProvided)
			},
		},
		{
			testCase:    "Test Case 3 - Usage",
			cpuID:       2,
			usageResult: &telemetryResult{42, nil},
			evalFn: func(v int, e error) bool {
				return assert.NoError(t, e) && assert.Equal(t, 42, v)
			},
		},
	}

	for _, tc := range tcases {
		t.Log(tc.testCase)

		cl := createNewDPDKTelemetryClient()
		if tc.usageResult != nil {
			cl.deltaUsage.Store(tc.cpuID, *tc.usageResult)
		}

		usage, err := cl.GetUsagePercent(tc.cpuID)

		tc.evalFn(usage, err)
	}
}

func TestDPDKTelemetryClient_Close(t *testing.T) {
	connectionCount := 3
	cancelCounter := 0

	t.Cleanup(func() {
		testHookCloseConnection = nil
	})
	testHookCloseConnection = func() {
		cancelCounter++
	}

	cl := createNewDPDKTelemetryClient()

	for i := 0; i < connectionCount; i++ {
		dummyConn := &dpdkTelemetryConnection{}
		cl.connections.Store(fmt.Sprint(i), dummyConn)
	}

	cl.Close()

	assert.Equal(t, connectionCount, cancelCounter)
	cl.connections.Range(func(key, value any) bool {
		t.Error("Connection map was not cleaned.")
		return false
	})
}

func createNewDPDKConnection() dpdkTelemetryConnection {
	log.SetLogger(zap.New(
		zap.UseDevMode(true),
		func(opts *zap.Options) {
			opts.TimeEncoder = zapcore.ISO8601TimeEncoder
		},
	))

	return dpdkTelemetryConnection{
		log:        ctrl.Log.WithName("test-log"),
		deltaUsage: &sync.Map{},
		cpuClasses: &sync.Map{},
	}
}

func TestDPDKConnection_close(t *testing.T) {
	cancelFuncCalled := false
	cpuList := []uint{1, 2, 3}

	dpdkConn := createNewDPDKConnection()
	dpdkConn.watchedCPUs = cpuList
	dpdkConn.cancelFunc = func() { cancelFuncCalled = true }
	for _, cpuID := range cpuList {
		dpdkConn.deltaUsage.Store(cpuID, nil)
		dpdkConn.cpuClasses.Store(cpuID, CPUClassPollingLcore)
	}

	dpdkConn.close()

	assert.True(t, cancelFuncCalled)
	dpdkConn.deltaUsage.Range(func(key, value any) bool {
		t.Error("Usage metrics map was not cleaned.")
		return false
	})
	dpdkConn.cpuClasses.Range(func(key, value any) bool {
		t.Error("CPU class map was not cleaned.")
		return false
	})
}

func TestDPDKConnection_connect(t *testing.T) {
	connectLoopCounter := 0
	t.Cleanup(func() {
		testHookStopConnectLoop = nil
	})
	testHookStopConnectLoop = func() bool {
		connectLoopCounter++
		return connectLoopCounter > 2
	}

	mkconn := &MockConn{}
	mkconn.On("Close").Return(nil)

	origRetryDuration := retryDuration
	origMetricsCooldown := samplePeriod
	origConnectFunc := connectWithTimeoutFunc
	t.Cleanup(func() {
		retryDuration = origRetryDuration
		samplePeriod = origMetricsCooldown
		connectWithTimeoutFunc = origConnectFunc
		testHookReadInitMsgReturn = nil
	})
	retryDuration = time.Millisecond
	samplePeriod = time.Millisecond
	connectWithTimeoutFunc = func(addr string, to time.Duration) (net.Conn, error) {
		return mkconn, nil
	}
	testHookReadInitMsgReturn = func() error {
		return fmt.Errorf("foo")
	}

	dpdkConn := createNewDPDKConnection()
	dpdkConn.podUID = "foo"
	dpdkConn.waitGroup.Add(1)

	ctx, cancel := context.WithCancel(context.TODO())
	t.Cleanup(cancel)

	dpdkConn.connect(ctx)

	mkconn.AssertCalled(t, "Close")
	assert.Panics(t, dpdkConn.waitGroup.Done)
}

func TestDPDKConnection_ioLoop(t *testing.T) {
	getMetricsCallCounter := 0
	t.Cleanup(func() {
		testHookReadInitMsgReturn = nil
		testHookHandleMetricsLoop = nil
		testHookDiscoveryLoop = nil
	})
	testHookReadInitMsgReturn = func() error {
		return nil
	}
	testHookDiscoveryLoop = func() error {
		return nil
	}
	testHookHandleMetricsLoop = func() error {
		getMetricsCallCounter++
		if getMetricsCallCounter > 2 {
			return fmt.Errorf("foo")
		}
		return nil
	}

	mkConn := &MockConn{}
	mkConn.On("Close").Return(nil)

	ctx, cancel := context.WithCancel(context.TODO())
	t.Cleanup(cancel)
	dpdkConn := createNewDPDKConnection()

	err := dpdkConn.ioLoop(ctx, mkConn)

	assert.NotNil(t, err)
	assert.ErrorContains(t, err, "foo")
	mkConn.AssertCalled(t, "Close")
}

func TestDPDKConnection_connectLoop(t *testing.T) {
	mkConn := &MockConn{}
	connectCallCounter := 0

	origConnectFunc := connectWithTimeoutFunc
	origRetryDuration := retryDuration
	t.Cleanup(func() {
		connectWithTimeoutFunc = origConnectFunc
		retryDuration = origRetryDuration
	})
	connectWithTimeoutFunc = func(addr string, to time.Duration) (net.Conn, error) {
		connectCallCounter++
		if connectCallCounter > 1 {
			return mkConn, nil
		}
		return nil, fmt.Errorf("foo")
	}
	retryDuration = time.Millisecond

	dpdkConn := createNewDPDKConnection()
	ctx, cancel := context.WithCancel(context.TODO())
	t.Cleanup(cancel)
	conn := dpdkConn.connectLoop(ctx)

	assert.NotNil(t, conn)
}

func TestDPDKConnection_processCommand(t *testing.T) {
	fooErr := fmt.Errorf("foo")
	command := "foo"
	commandBytes := []byte(command)
	timestamp := time.Now()
	timeout := timestamp.Add(ioTimeout)
	origDeadlineFunc := getCurrentTimestamp
	t.Cleanup(func() {
		getCurrentTimestamp = origDeadlineFunc
	})
	getCurrentTimestamp = func() time.Time {
		return timestamp
	}

	tcases := []struct {
		testCase     string
		connSetup    func() *MockConn
		expectedData usageResponse
		evalFn       func(e error, d usageResponse, c *MockConn)
	}{
		{
			testCase: "Test Case 1 - Write deadline error",
			connSetup: func() *MockConn {
				mc := MockConn{}
				mc.On("SetWriteDeadline", timeout).Return(fooErr)
				return &mc
			},
			evalFn: func(e error, d usageResponse, c *MockConn) {
				assert.NotNil(t, e)
				assert.Empty(t, d)
				assert.ErrorContains(t, e, "error setting write deadline")
				c.AssertCalled(t, "SetWriteDeadline", timeout)
				c.AssertNotCalled(t, "Write")
			},
		},
		{
			testCase: "Test Case 2 - Write error",
			connSetup: func() *MockConn {
				mc := MockConn{}
				mc.On("SetWriteDeadline", timeout).Return(nil)
				mc.On("Write", commandBytes).Return(0, fooErr)
				return &mc
			},
			evalFn: func(e error, d usageResponse, c *MockConn) {
				assert.NotNil(t, e)
				assert.Empty(t, d)
				assert.ErrorContains(t, e, "write error")
				c.AssertCalled(t, "SetWriteDeadline", timeout)
				c.AssertCalled(t, "Write", commandBytes)
				c.AssertNotCalled(t, "Read")
			},
		},
		{
			testCase: "Test Case 3 - Read deadline error",
			connSetup: func() *MockConn {
				mc := MockConn{}
				mc.On("SetWriteDeadline", timeout).Return(nil)
				mc.On("Write", commandBytes).Return(0, nil)
				mc.On("SetReadDeadline", timeout).Return(fooErr)
				return &mc
			},
			evalFn: func(e error, d usageResponse, c *MockConn) {
				assert.NotNil(t, e)
				assert.Empty(t, d)
				assert.ErrorContains(t, e, "error setting read deadline")
				c.AssertCalled(t, "SetWriteDeadline", timeout)
				c.AssertCalled(t, "Write", commandBytes)
				c.AssertCalled(t, "SetReadDeadline", timeout)
				c.AssertNotCalled(t, "Read")
			},
		},
		{
			testCase: "Test Case 4 - Read error",
			connSetup: func() *MockConn {
				mc := MockConn{}
				mc.On("SetWriteDeadline", timeout).Return(nil)
				mc.On("Write", commandBytes).Return(0, nil)
				mc.On("SetReadDeadline", timeout).Return(nil)
				mc.On("Read", mock.Anything).Return(0, fooErr, []byte(""))
				return &mc
			},
			evalFn: func(e error, d usageResponse, c *MockConn) {
				assert.NotNil(t, e)
				assert.Empty(t, d)
				assert.ErrorContains(t, e, "read error")
				c.AssertCalled(t, "SetWriteDeadline", timeout)
				c.AssertCalled(t, "Write", commandBytes)
				c.AssertCalled(t, "SetReadDeadline", timeout)
				// Anything is used here instead of AnythingOfType because assert
				// still makes a distinction between filled and empty slice.
				c.AssertCalled(t, "Read", mock.Anything)
			},
		},
		{
			testCase: "Test Case 5 - Malformed JSON response",
			connSetup: func() *MockConn {
				mc := MockConn{}
				mc.On("SetWriteDeadline", timeout).Return(nil)
				mc.On("Write", commandBytes).Return(0, nil)
				mc.On("SetReadDeadline", timeout).Return(nil)
				mc.On("Read", mock.Anything).Return(3, nil, commandBytes)
				return &mc
			},
			evalFn: func(e error, d usageResponse, c *MockConn) {
				assert.ErrorContains(t, e, "invalid character")
				assert.Empty(t, d)
				c.AssertCalled(t, "SetWriteDeadline", timeout)
				c.AssertCalled(t, "Write", commandBytes)
				c.AssertCalled(t, "SetReadDeadline", timeout)
				// Anything is used here instead of AnythingOfType because assert
				// still makes a distinction between filled and empty slice.
				c.AssertCalled(t, "Read", mock.Anything)
			},
		},
		{
			testCase: "Test Case 6 - Success",
			connSetup: func() *MockConn {
				mc := MockConn{}
				mc.On("SetWriteDeadline", timeout).Return(nil)
				mc.On("Write", commandBytes).Return(0, nil)
				mc.On("SetReadDeadline", timeout).Return(nil)
				mc.On("Read", mock.Anything).Return(len(usageOutputString), nil, []byte(usageOutputString))
				return &mc
			},
			expectedData: usageResponse{
				Usage: usageData{
					LcoreIDs:    []uint{1, 2, 3},
					TotalCycles: []uint64{2142696834760, 2142696698860, 2142696553160},
					BusyCycles:  []uint64{1704128133420, 1704896313700, 1714666267580},
				},
			},
			evalFn: func(e error, d usageResponse, c *MockConn) {
				assert.NoError(t, e)
				assert.NotEmpty(t, d)
				c.AssertCalled(t, "SetWriteDeadline", timeout)
				c.AssertCalled(t, "Write", commandBytes)
				c.AssertCalled(t, "SetReadDeadline", timeout)
				// Anything is used here instead of AnythingOfType because assert
				// still makes a distinction between filled and empty slice.
				c.AssertCalled(t, "Read", mock.Anything)
			},
		},
	}

	for _, tc := range tcases {
		t.Log(tc.testCase)

		dpdkConn := createNewDPDKConnection()
		dpdkConn.buffer = make([]byte, 256)
		conn := tc.connSetup()

		data := usageResponse{}
		err := dpdkConn.processCommand(conn, command, &data)

		tc.evalFn(err, data, conn)
		if len(tc.expectedData.Usage.TotalCycles) != 0 {
			assert.Equal(t, tc.expectedData, data)
		}
	}
}

func TestDPDKConnection_readInitialMessage(t *testing.T) {
	t.Cleanup(func() {
		testHookProcessCommandReturn = nil
	})

	tcases := []struct {
		TestCase string
		data     initialMessage
		err      error
		evalFn   func(e error, buflen int)
	}{
		{
			TestCase: "Test Case 1 - Read Deadline error",
			err:      fmt.Errorf("foo"),
			evalFn: func(e error, buflen int) {
				assert.NotNil(t, e)
				assert.ErrorContains(t, e, "initial message error:")
			},
		},
		{
			TestCase: "Test Case 2 - Success",
			data: initialMessage{
				Version:         "1",
				PID:             1,
				MaxOutputLength: 10,
			},
			evalFn: func(e error, buflen int) {
				assert.NoError(t, e)
				assert.Equal(t, 10, buflen)
			},
		},
	}
	for _, tc := range tcases {
		t.Log(tc.TestCase)

		usedCommand := ""
		testHookProcessCommandReturn = func(cmd string) (any, error) {
			usedCommand = cmd
			return tc.data, tc.err
		}

		mkConn := &MockConn{}
		dpdkConn := createNewDPDKConnection()
		err := dpdkConn.handleInitialMessage(mkConn)

		assert.Equal(t, "", usedCommand)
		tc.evalFn(err, len(dpdkConn.buffer))
	}
}

func TestDPDKConnection_handleUsage(t *testing.T) {
	t.Cleanup(func() {
		testHookProcessCommandReturn = nil
	})

	tcases := []struct {
		testCase         string
		initialUsage     metricsMap
		expectedUsage    metricsMap
		watchlist        []uint
		lcoreIDByCPU     map[uint]uint
		prevUsageCycles  map[uint]cycleCounters
		curUsageResponse usageResponse
		err              error
		evalFn           func(e error, expected metricsMap, b *sync.Map)
	}{
		{
			testCase: "Test Case 1 - Command processing error",
			err:      fmt.Errorf("foo"),
			evalFn: func(e error, expected metricsMap, b *sync.Map) {
				assert.NotNil(t, e)
				assert.ErrorContains(t, e, "usage error:")
			},
		},
		{
			testCase: "Test Case 2 - Usage not available",
			expectedUsage: metricsMap{
				1: telemetryResult{0, ErrDPDKMetricNotProvided},
			},
			watchlist:    []uint{1},
			lcoreIDByCPU: map[uint]uint{1: 1},
			evalFn: func(e error, expected metricsMap, b *sync.Map) {
				assert.NoError(t, e)
				for id, expVal := range expected {
					val, found := b.Load(id)
					assert.True(t, found)
					assert.Equal(t, expVal, val.(telemetryResult))
				}
			},
		},
		{
			testCase: "Test Case 3 - Update usage readings",
			initialUsage: metricsMap{
				1: telemetryResult{88, nil},
				2: telemetryResult{0, nil},
				3: telemetryResult{56, nil},
			},
			expectedUsage: metricsMap{
				1: telemetryResult{88, nil},
				2: telemetryResult{89, nil},
				3: telemetryResult{0, ErrDPDKMetricNotProvided},
				4: telemetryResult{0, ErrDPDKMetricNotProvided},
			},
			prevUsageCycles: map[uint]cycleCounters{
				1: {total: 23846845490, busy: 21043446594},
				2: {total: 23900558814, busy: 21448837227},
			},
			watchlist:    []uint{1, 2, 3, 4},
			lcoreIDByCPU: map[uint]uint{1: 1, 2: 2, 3: 3, 4: 4},
			curUsageResponse: usageResponse{
				Usage: usageData{
					LcoreIDs:    []uint{1, 2},
					TotalCycles: []uint64{23846845590, 23900558914},
					BusyCycles:  []uint64{21043446682, 21448837316},
				},
			},
			evalFn: func(e error, expected metricsMap, b *sync.Map) {
				assert.NoError(t, e)
				for id, expVal := range expected {
					val, found := b.Load(id)
					assert.True(t, found)
					assert.Equal(t, expVal, val.(telemetryResult))
				}
			},
		},
		{
			// Lcore IDs are not host CPU IDs unless the application happens to
			// map them identically; usage must be attributed via the discovered
			// mapping, not by treating the reported lcore ID as a CPU ID.
			testCase: "Test Case 4 - Lcore IDs differ from CPU IDs",
			expectedUsage: metricsMap{
				10: telemetryResult{88, nil},
				11: telemetryResult{0, ErrDPDKMetricNotProvided},
			},
			prevUsageCycles: map[uint]cycleCounters{
				10: {total: 23846845490, busy: 21043446594},
			},
			watchlist:    []uint{10, 11},
			lcoreIDByCPU: map[uint]uint{10: 0, 11: 1},
			curUsageResponse: usageResponse{
				Usage: usageData{
					LcoreIDs:    []uint{0},
					TotalCycles: []uint64{23846845590},
					BusyCycles:  []uint64{21043446682},
				},
			},
			evalFn: func(e error, expected metricsMap, b *sync.Map) {
				assert.NoError(t, e)
				for id, expVal := range expected {
					val, found := b.Load(id)
					assert.True(t, found)
					assert.Equal(t, expVal, val.(telemetryResult))
				}
			},
		},
	}

	for _, tc := range tcases {
		t.Log(tc.testCase)

		usedCommand := ""
		testHookProcessCommandReturn = func(cmd string) (any, error) {
			usedCommand = cmd
			return tc.curUsageResponse, tc.err
		}

		mkConn := &MockConn{}
		dpdkConn := createNewDPDKConnection()
		dpdkConn.watchedCPUs = tc.watchlist
		dpdkConn.lcoreIDByCPU = tc.lcoreIDByCPU

		for id, val := range tc.prevUsageCycles {
			dpdkConn.prevUsageCycles.Store(id, val)
		}

		for id, val := range tc.initialUsage {
			dpdkConn.deltaUsage.Store(id, val)
		}

		err := dpdkConn.handleUsage(mkConn)

		assert.Equal(t, usageCommand, usedCommand)
		tc.evalFn(err, tc.expectedUsage, dpdkConn.deltaUsage)
	}
}

func TestDPDKConnection_handleDiscovery(t *testing.T) {
	t.Cleanup(func() {
		testHookProcessCommandReturn = nil
	})

	tcases := []struct {
		testCase      string
		watchlist     []uint
		lcoreList     []uint
		lcoreInfo     map[uint]lcoreInfo
		usage         usageResponse
		expectedClass map[uint]CPUClass
		expectedLcore map[uint]uint
	}{
		{
			// CPUs 0 and 2 run forwarding lcores, CPU 1 is the main lcore and
			// never reports usage, CPU 3 is in the cpuset but is not an lcore.
			testCase:  "classifies polling, silent and non-lcore CPUs",
			watchlist: []uint{0, 1, 2, 3},
			lcoreList: []uint{0, 1, 2},
			lcoreInfo: map[uint]lcoreInfo{
				0: {LcoreID: 0, CPUSet: []uint{0}},
				1: {LcoreID: 1, CPUSet: []uint{1}},
				2: {LcoreID: 2, CPUSet: []uint{2}},
			},
			usage: usageResponse{Usage: usageData{LcoreIDs: []uint{0, 2}}},
			expectedClass: map[uint]CPUClass{
				0: CPUClassPollingLcore,
				1: CPUClassSilentLcore,
				2: CPUClassPollingLcore,
				3: CPUClassNonLcore,
			},
			expectedLcore: map[uint]uint{0: 0, 1: 1, 2: 2},
		},
		{
			// Lcore IDs need not match CPU IDs; the cpuset of each lcore is
			// what ties an lcore to a host CPU.
			testCase:  "maps lcores to CPUs through their cpuset",
			watchlist: []uint{10, 11},
			lcoreList: []uint{0, 1},
			lcoreInfo: map[uint]lcoreInfo{
				0: {LcoreID: 0, CPUSet: []uint{10}},
				1: {LcoreID: 1, CPUSet: []uint{11}},
			},
			usage: usageResponse{Usage: usageData{LcoreIDs: []uint{1}}},
			expectedClass: map[uint]CPUClass{
				10: CPUClassSilentLcore,
				11: CPUClassPollingLcore,
			},
			expectedLcore: map[uint]uint{10: 0, 11: 1},
		},
		{
			// Lcore 0 floats over CPUs 0 and 1, so its load lands wherever it
			// likes. Scaling CPU 1 on lcore 1's usage alone would miss the
			// work lcore 0 puts there, and CPU 0 would look like a plain
			// non-lcore and could be driven to the hardware minimum under a
			// running lcore. One unpinned lcore therefore stops the whole
			// container from being scaled, including CPU 2, which is pinned
			// correctly and would otherwise be fine.
			testCase:  "an unpinned lcore makes the whole container unscalable",
			watchlist: []uint{0, 1, 2},
			lcoreList: []uint{0, 1, 2},
			lcoreInfo: map[uint]lcoreInfo{
				0: {LcoreID: 0, CPUSet: []uint{0, 1}},
				1: {LcoreID: 1, CPUSet: []uint{1}},
				2: {LcoreID: 2, CPUSet: []uint{2}},
			},
			usage: usageResponse{Usage: usageData{LcoreIDs: []uint{0, 1, 2}}},
			expectedClass: map[uint]CPUClass{
				0: CPUClassUnsupported,
				1: CPUClassUnsupported,
				2: CPUClassUnsupported,
			},
			expectedLcore: map[uint]uint{},
		},
		{
			// An lcore EAL has disabled is skipped by rte_lcore_iterate, so
			// asking for its info yields an empty body. It is not an unpinned
			// lcore and must not take the rest of the container down with it.
			testCase:  "an lcore with an empty info response is skipped",
			watchlist: []uint{0, 1},
			lcoreList: []uint{0, 1},
			lcoreInfo: map[uint]lcoreInfo{
				0: {LcoreID: 0, CPUSet: []uint{0}},
				1: {},
			},
			usage: usageResponse{Usage: usageData{LcoreIDs: []uint{0}}},
			expectedClass: map[uint]CPUClass{
				0: CPUClassPollingLcore,
				1: CPUClassNonLcore,
			},
			expectedLcore: map[uint]uint{0: 0},
		},
		{
			// Several lcores on one CPU: each reports only part of what the
			// CPU is doing, so scaling on any one of them would read the CPU
			// as idler than it is. CPU 2 keeps its own scalable lcore.
			testCase:  "a CPU running several lcores is not scaled",
			watchlist: []uint{0, 1, 2},
			lcoreList: []uint{0, 1, 2, 3},
			lcoreInfo: map[uint]lcoreInfo{
				0: {LcoreID: 0, CPUSet: []uint{0}},
				1: {LcoreID: 1, CPUSet: []uint{0}},
				2: {LcoreID: 2, CPUSet: []uint{2}},
				3: {LcoreID: 3, CPUSet: []uint{0}},
			},
			usage: usageResponse{Usage: usageData{LcoreIDs: []uint{0, 1, 2, 3}}},
			expectedClass: map[uint]CPUClass{
				0: CPUClassSilentLcore,
				1: CPUClassNonLcore,
				2: CPUClassPollingLcore,
			},
			expectedLcore: map[uint]uint{2: 2},
		},
		{
			// DPDK below 23.03 has no lcore endpoints. Nothing can be
			// discovered about such an application, so none of its CPUs can be
			// scaled; requesting polling-dpdk scaling for it is a config error.
			testCase:  "marks every CPU unsupported without the lcore endpoints",
			watchlist: []uint{4, 5},
			lcoreList: nil,
			expectedClass: map[uint]CPUClass{
				4: CPUClassUnsupported,
				5: CPUClassUnsupported,
			},
			expectedLcore: map[uint]uint{},
		},
		{
			// Reporting usage is opt-in for the application. Without the
			// callback the lcores are still discovered, but none of them can
			// be scaled; this too is a config error.
			testCase:  "leaves every lcore silent without a usage callback",
			watchlist: []uint{0, 1},
			lcoreList: []uint{0},
			lcoreInfo: map[uint]lcoreInfo{
				0: {LcoreID: 0, CPUSet: []uint{0}},
			},
			usage: usageResponse{},
			expectedClass: map[uint]CPUClass{
				0: CPUClassSilentLcore,
				1: CPUClassNonLcore,
			},
			expectedLcore: map[uint]uint{0: 0},
		},
	}

	for _, tc := range tcases {
		t.Run(tc.testCase, func(t *testing.T) {
			testHookProcessCommandReturn = func(cmd string) (any, error) {
				switch cmd {
				case lcoreListCommand:
					return lcoreListResponse{LcoreIDs: tc.lcoreList}, nil
				case usageCommand:
					return tc.usage, nil
				}
				for lcoreID, info := range tc.lcoreInfo {
					if cmd == fmt.Sprintf(lcoreInfoCommand, lcoreID) {
						return lcoreInfoResponse{Info: info}, nil
					}
				}
				return nil, fmt.Errorf("unexpected command %q", cmd)
			}

			dpdkConn := createNewDPDKConnection()
			dpdkConn.watchedCPUs = tc.watchlist
			dpdkConn.pollingLcores = map[uint]struct{}{}
			dpdkConn.loggedSharedCPU = map[uint]struct{}{}

			assert.NoError(t, dpdkConn.handleDiscovery(&MockConn{}))

			for cpuID, expected := range tc.expectedClass {
				class, found := dpdkConn.cpuClasses.Load(cpuID)
				assert.True(t, found, "no class stored for cpu %d", cpuID)
				assert.Equal(t, expected, class, "cpu %d", cpuID)
			}
			assert.Equal(t, tc.expectedLcore, dpdkConn.lcoreIDByCPU)
		})
	}
}

// TestDPDKConnection_handleDiscovery_PollingIsSticky ensures a lcore whose
// usage callback fails for one discovery pass is not reclassified as silent,
// which would drop it out of dynamic scaling.
func TestDPDKConnection_handleDiscovery_PollingIsSticky(t *testing.T) {
	t.Cleanup(func() {
		testHookProcessCommandReturn = nil
	})

	reportUsage := true
	testHookProcessCommandReturn = func(cmd string) (any, error) {
		switch cmd {
		case lcoreListCommand:
			return lcoreListResponse{LcoreIDs: []uint{0}}, nil
		case fmt.Sprintf(lcoreInfoCommand, 0):
			return lcoreInfoResponse{Info: lcoreInfo{LcoreID: 0, CPUSet: []uint{0}}}, nil
		case usageCommand:
			if reportUsage {
				return usageResponse{Usage: usageData{LcoreIDs: []uint{0}}}, nil
			}
			return usageResponse{}, nil
		}
		return nil, fmt.Errorf("unexpected command %q", cmd)
	}

	dpdkConn := createNewDPDKConnection()
	dpdkConn.watchedCPUs = []uint{0}
	dpdkConn.pollingLcores = map[uint]struct{}{}
	dpdkConn.loggedSharedCPU = map[uint]struct{}{}

	assert.NoError(t, dpdkConn.handleDiscovery(&MockConn{}))
	class, _ := dpdkConn.cpuClasses.Load(uint(0))
	assert.Equal(t, CPUClassPollingLcore, class)

	reportUsage = false
	assert.NoError(t, dpdkConn.handleDiscovery(&MockConn{}))
	class, _ = dpdkConn.cpuClasses.Load(uint(0))
	assert.Equal(t, CPUClassPollingLcore, class)
}

// errorCapturingSink collects the errors logged through it, so that a test can
// assert on what a connection reported and how often.
type errorCapturingSink struct {
	errs *[]error
}

func (s errorCapturingSink) Init(logr.RuntimeInfo)               {}
func (s errorCapturingSink) Enabled(int) bool                    { return false }
func (s errorCapturingSink) Info(int, string, ...any)            {}
func (s errorCapturingSink) WithValues(...any) logr.LogSink      { return s }
func (s errorCapturingSink) WithName(string) logr.LogSink        { return s }
func (s errorCapturingSink) Error(err error, _ string, _ ...any) { *s.errs = append(*s.errs, err) }

// TestDPDKConnection_handleDiscovery_ConfigErrors checks that an application
// CPM cannot scale is reported as a configuration error, and that it is
// reported once per connection rather than on every discovery pass.
func TestDPDKConnection_handleDiscovery_ConfigErrors(t *testing.T) {
	t.Cleanup(func() {
		testHookProcessCommandReturn = nil
	})

	tcases := []struct {
		testCase    string
		lcoreList   []uint
		cpuSet      []uint
		expectedErr error
	}{
		{
			// DPDK below 23.03 exposes none of the lcore endpoints.
			testCase:    "dpdk without the lcore endpoints",
			lcoreList:   nil,
			expectedErr: ErrDPDKNoLcoreEndpoints,
		},
		{
			// The endpoints are there, but the application never registered a
			// usage callback, so no lcore reports usage.
			testCase:    "application without a usage callback",
			lcoreList:   []uint{0},
			cpuSet:      []uint{0},
			expectedErr: ErrDPDKNoLcoreUsage,
		},
		{
			// The lcore is free to move between CPUs, so its usage cannot be
			// attributed to any one of them.
			testCase:    "application with an unpinned lcore",
			lcoreList:   []uint{0},
			cpuSet:      []uint{0, 1},
			expectedErr: ErrDPDKUnpinnedLcore,
		},
	}

	for _, tc := range tcases {
		t.Run(tc.testCase, func(t *testing.T) {
			testHookProcessCommandReturn = func(cmd string) (any, error) {
				switch cmd {
				case lcoreListCommand:
					return lcoreListResponse{LcoreIDs: tc.lcoreList}, nil
				case fmt.Sprintf(lcoreInfoCommand, 0):
					return lcoreInfoResponse{Info: lcoreInfo{LcoreID: 0, CPUSet: tc.cpuSet}}, nil
				case usageCommand:
					return usageResponse{}, nil
				}
				return nil, fmt.Errorf("unexpected command %q", cmd)
			}

			var logged []error
			dpdkConn := createNewDPDKConnection()
			dpdkConn.log = logr.New(errorCapturingSink{errs: &logged})
			dpdkConn.watchedCPUs = []uint{0}
			dpdkConn.pollingLcores = map[uint]struct{}{}
			dpdkConn.loggedSharedCPU = map[uint]struct{}{}

			// Two passes: the error describes the application, not this tick,
			// so repeating it every second would only flood the log.
			assert.NoError(t, dpdkConn.handleDiscovery(&MockConn{}))
			assert.NoError(t, dpdkConn.handleDiscovery(&MockConn{}))

			reported := 0
			for _, err := range logged {
				if errors.Is(err, tc.expectedErr) {
					reported++
				}
			}
			assert.Equal(t, 1, reported, "expected the config error exactly once, got %v", logged)
		})
	}
}
