package scaling

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/go-logr/logr"
)

const (
	baseSocketPath   = "/var/lib/power-node-agent/pods/%s/dpdk/rte/dpdk_telemetry.v2"
	usageCommand     = "/eal/lcore/usage"
	lcoreListCommand = "/eal/lcore/list"
	lcoreInfoCommand = "/eal/lcore/info,%d"
	ioTimeout        = 3 * time.Second
)

// CPUClass describes the role a CPU of a DPDK container plays, as discovered
// from the DPDK telemetry lcore endpoints. The scaling updater drives each
// class with a different frequency.
type CPUClass int

const (
	// CPUClassUnknown means lcore discovery has not completed for this CPU yet
	// (telemetry not connected, or first discovery pass still pending).
	CPUClassUnknown CPUClass = iota
	// CPUClassPollingLcore is a DPDK lcore that reports usage, so it can be scaled dynamically.
	CPUClassPollingLcore
	// CPUClassSilentLcore is a CPU running one or more DPDK lcores that cannot
	// be scaled: either its lcore reports no usage at all, most commonly the
	// main lcore, or it runs several lcores and no one usage figure describes
	// what the CPU is doing.
	CPUClassSilentLcore
	// CPUClassNonLcore is a CPU of the container's cpuset that has no DPDK
	// lcore pinned to it.
	CPUClassNonLcore
	// CPUClassUnsupported is a CPU of an application none of whose CPUs can be
	// scaled: its DPDK predates the lcore telemetry endpoints, or its lcores
	// are not pinned one to a CPU.
	CPUClassUnsupported
)

func (c CPUClass) String() string {
	switch c {
	case CPUClassPollingLcore:
		return "polling-lcore"
	case CPUClassSilentLcore:
		return "silent-lcore"
	case CPUClassNonLcore:
		return "non-lcore"
	case CPUClassUnsupported:
		return "unsupported"
	default:
		return "unknown"
	}
}

var (
	ErrDPDKMetricMissing     = errors.New("no entry found for this cpu")
	ErrDPDKMetricNotProvided = errors.New("dpdk telemetry did not provide a reading for this cpu")

	// Requesting polling-dpdk scaling for an application that cannot report
	// what CPM needs is a configuration error. Neither is fatal to the
	// telemetry connection; each is reported once per connection and leaves
	// the container's CPUs at the application frequency.
	ErrDPDKNoLcoreEndpoints = errors.New(
		"dpdk telemetry does not expose the lcore endpoints, which were added in DPDK 23.03")
	ErrDPDKNoLcoreUsage = errors.New(
		"no dpdk lcore reports usage, the application does not call rte_lcore_register_usage_cb()")
	ErrDPDKUnpinnedLcore = errors.New(
		"dpdk lcores are not pinned one to a CPU, so their usage cannot be attributed to a CPU")

	retryDuration = 1 * time.Second
	// Metric polling period; also the sampling window for windowed usage.
	samplePeriod = 10 * time.Millisecond
	// Period at which the lcore layout of the application is rediscovered.
	// Lcores are static once EAL has initialised, but a usage callback may be
	// registered later, which moves an lcore from silent to polling.
	discoveryPeriod = 1 * time.Second

	connectWithTimeoutFunc = connectWithTimeout
	getCurrentTimestamp    = time.Now

	testHookReadInitMsgReturn    func() error
	testHookHandleMetricsLoop    func() error
	testHookDiscoveryLoop        func() error
	testHookProcessCommandReturn func(cmd string) (any, error)
	testHookNewSocketConnection  func(conn *dpdkTelemetryConnection)
	testHookStopConnectLoop      func() bool
	testHookCloseConnection      func()
)

type initialMessage struct {
	Version         string `json:"version"`
	PID             int    `json:"pid"`
	MaxOutputLength int    `json:"max_output_len"`
}
type usageResponse struct {
	Usage usageData `json:"/eal/lcore/usage"`
}
type usageData struct {
	LcoreIDs    []uint   `json:"lcore_ids"`
	TotalCycles []uint64 `json:"total_cycles"`
	BusyCycles  []uint64 `json:"busy_cycles"`
}
type lcoreListResponse struct {
	LcoreIDs []uint `json:"/eal/lcore/list"`
}
type lcoreInfoResponse struct {
	Info lcoreInfo `json:"/eal/lcore/info"`
}
type lcoreInfo struct {
	LcoreID uint   `json:"lcore_id"`
	CPUSet  []uint `json:"cpuset"`
}

// cycleCounters holds cumulative cycle counters reported by DPDK usage endpoint for an lcore.
type cycleCounters struct {
	total uint64
	busy  uint64
}

func connectWithTimeout(addr string, to time.Duration) (net.Conn, error) {
	return net.DialTimeout("unixpacket", addr, to)
}

type DPDKTelemetryConnectionData struct {
	PodUID      string
	WatchedCPUs []uint
}

type DPDKTelemetryClient interface {
	EnsureConnection(data *DPDKTelemetryConnectionData)
	ListConnections() []DPDKTelemetryConnectionData
	CloseConnection(podUID string)
	GetUsagePercent(cpuID uint) (int, error)
	GetCPUClass(cpuID uint) CPUClass
	Close()
}

type dpdkTelemetryClientImpl struct {
	log         logr.Logger
	connections sync.Map
	deltaUsage  sync.Map
	cpuClasses  sync.Map
}

func NewDPDKTelemetryClient(logger logr.Logger) DPDKTelemetryClient {
	c := &dpdkTelemetryClientImpl{
		log: logger,
	}

	return c
}

func (cl *dpdkTelemetryClientImpl) EnsureConnection(data *DPDKTelemetryConnectionData) {
	cl.log.V(4).Info("ensuring connection", "podUID", data.PodUID, "watchedCPUs", data.WatchedCPUs)
	ctx, cancel := context.WithCancel(context.Background())
	podUID := data.PodUID
	newConn := &dpdkTelemetryConnection{
		podUID:      podUID,
		watchedCPUs: data.WatchedCPUs,
		deltaUsage:  &cl.deltaUsage,
		cpuClasses:  &cl.cpuClasses,
		log:         cl.log.WithValues("podUID", podUID),
		cancelFunc:  cancel,
	}

	if _, present := cl.connections.LoadOrStore(podUID, newConn); !present {
		if testHookNewSocketConnection != nil {
			testHookNewSocketConnection(newConn)
		} else {
			newConn.waitGroup.Add(1)
			go newConn.connect(ctx)
		}
	}
}

func (cl *dpdkTelemetryClientImpl) ListConnections() []DPDKTelemetryConnectionData {
	dataList := make([]DPDKTelemetryConnectionData, 0)

	cl.connections.Range(func(key, value any) bool {
		connection := value.(*dpdkTelemetryConnection)
		dataList = append(dataList, DPDKTelemetryConnectionData{
			PodUID:      connection.podUID,
			WatchedCPUs: connection.watchedCPUs,
		})
		return true
	})

	return dataList
}

func (cl *dpdkTelemetryClientImpl) CloseConnection(podUID string) {
	if connection, found := cl.connections.LoadAndDelete(podUID); found {
		connection := connection.(*dpdkTelemetryConnection)
		connection.close()
		cl.log.V(4).Info("stopped the connection.", "podUID", podUID)
	} else {
		cl.log.V(4).Info("connection does not exist.", "podUID", podUID)
	}
}

func (cl *dpdkTelemetryClientImpl) GetUsagePercent(cpuID uint) (int, error) {
	if value, found := cl.deltaUsage.Load(cpuID); found {
		r := value.(telemetryResult)
		return r.percent, r.err
	}

	return 0, ErrDPDKMetricMissing
}

// GetCPUClass returns the discovered role of a CPU. CPUClassUnknown is returned
// while the telemetry connection is down or the first discovery pass has not
// completed yet.
func (cl *dpdkTelemetryClientImpl) GetCPUClass(cpuID uint) CPUClass {
	if value, found := cl.cpuClasses.Load(cpuID); found {
		return value.(CPUClass)
	}

	return CPUClassUnknown
}

func (cl *dpdkTelemetryClientImpl) Close() {
	cl.log.V(4).Info("stopping all connection loops.")

	cl.connections.Range(func(key, value any) bool {
		connection := value.(*dpdkTelemetryConnection)
		connection.close()
		cl.connections.Delete(key.(string))
		return true
	})

	cl.log.V(4).Info("all connection loops stopped.")
}

type dpdkTelemetryConnection struct {
	podUID          string
	watchedCPUs     []uint
	prevUsageCycles sync.Map  // last cumulative usage counters per CPU
	deltaUsage      *sync.Map // latest windowed usage percent per CPU
	cpuClasses      *sync.Map // discovered CPUClass per CPU
	buffer          []byte
	log             logr.Logger
	waitGroup       sync.WaitGroup
	cancelFunc      func()
	// lcoreIDByCPU maps a host CPU ID to the DPDK lcore ID pinned to it. Written
	// by the discovery pass and read by handleUsage; both run on the io loop
	// goroutine, so no locking is needed.
	lcoreIDByCPU map[uint]uint
	// pollingLcores holds the lcore IDs seen reporting usage during this
	// connection. Membership is sticky so that a momentarily failing usage
	// callback does not reclassify a polling lcore as silent.
	pollingLcores      map[uint]struct{}
	loggedNoLcoreAPI   bool
	loggedNoLcoreUsage bool
	loggedUnpinned     bool
	loggedSharedCPU    map[uint]struct{}
}

// telemetryResult represents the latest windowed CPU usage sample.
// percent: 0–100 computed from delta(busy_cycles)/delta(total_cycles) over samplePeriod.
// err: non-nil when the sample is unavailable
type telemetryResult struct {
	percent int
	err     error
}

func (c *dpdkTelemetryConnection) close() {
	if testHookCloseConnection != nil {
		testHookCloseConnection()
		return
	}

	c.cancelFunc()
	c.waitGroup.Wait()
	c.clearMetrics(c.watchedCPUs)
}

func (c *dpdkTelemetryConnection) connect(ctx context.Context) {
	defer c.waitGroup.Done()

	for {
		if testHookStopConnectLoop != nil {
			if testHookStopConnectLoop() {
				return
			}
		}
		conn := c.connectLoop(ctx)
		if conn == nil {
			c.log.V(4).Info("client loop finished")
			return
		}
		if err := c.ioLoop(ctx, conn); err != nil {
			c.clearMetrics(c.watchedCPUs)
			c.log.Error(err, "connection closed")
		}
	}
}

func (c *dpdkTelemetryConnection) connectLoop(ctx context.Context) net.Conn {
	address := fmt.Sprintf(baseSocketPath, c.podUID)
	loggedWait := false
	loggedUnexpected := false

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(retryDuration):
			conn, err := connectWithTimeoutFunc(address, ioTimeout)
			if err == nil {
				c.log.Info("connected to dpdk telemetry socket")
				return conn
			}
			// Socket file not created yet; other errors are still retried but logged as failures.
			if errors.Is(err, os.ErrNotExist) {
				if !loggedWait {
					c.log.Info("waiting for dpdk telemetry socket to become available", "socket", address)
					loggedWait = true
				}
				continue
			}
			if !loggedUnexpected {
				c.log.Error(err, "failed to connect to dpdk telemetry socket")
				loggedUnexpected = true
			}
		}
	}
}

// ioLoop polls the DPDK telemetry socket on a fixed schedule.
func (c *dpdkTelemetryConnection) ioLoop(ctx context.Context, conn net.Conn) error {
	defer conn.Close()

	if err := c.handleInitialMessage(conn); err != nil {
		return err
	}

	// Lcore layout is a property of the application process behind this socket,
	// so it is rediscovered from scratch for every new connection.
	c.lcoreIDByCPU = map[uint]uint{}
	c.pollingLcores = map[uint]struct{}{}
	c.loggedSharedCPU = map[uint]struct{}{}
	c.loggedNoLcoreAPI = false
	c.loggedNoLcoreUsage = false
	c.loggedUnpinned = false

	// Classify before the first usage poll so that CPUs are not left Unknown
	// for a whole discovery period.
	if err := c.handleDiscovery(conn); err != nil {
		return err
	}

	usageTicker := time.NewTicker(samplePeriod)
	defer usageTicker.Stop()
	discoveryTicker := time.NewTicker(discoveryPeriod)
	defer discoveryTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-discoveryTicker.C:
			if err := c.handleDiscovery(conn); err != nil {
				return err
			}
		case <-usageTicker.C:
			if err := c.handleUsage(conn); err != nil {
				return err
			}
		}
	}
}

func (c *dpdkTelemetryConnection) processCommand(conn net.Conn, cmd string, v any) error {
	var err error

	if testHookProcessCommandReturn != nil {
		value, e := testHookProcessCommandReturn(cmd)
		if e != nil {
			return e
		}
		rv := reflect.ValueOf(v)
		rv.Elem().Set(reflect.ValueOf(value))
		return nil
	}

	if cmd != "" {
		if err = conn.SetWriteDeadline(
			getCurrentTimestamp().Add(ioTimeout),
		); err != nil {
			return fmt.Errorf("error setting write deadline: %w", err)
		}
		_, err = conn.Write([]byte(cmd))
		if err != nil {
			return fmt.Errorf("write error: %w", err)
		}
	}

	if err := conn.SetReadDeadline(
		getCurrentTimestamp().Add(ioTimeout),
	); err != nil {
		return fmt.Errorf("error setting read deadline: %w", err)
	}
	bytesRead, err := conn.Read(c.buffer)
	if err != nil {
		return fmt.Errorf("read error: %w", err)
	}

	if err := json.Unmarshal(c.buffer[:bytesRead], v); err != nil {
		return err
	}

	return nil
}

func (c *dpdkTelemetryConnection) handleInitialMessage(conn net.Conn) error {
	if testHookReadInitMsgReturn != nil {
		return testHookReadInitMsgReturn()
	}

	c.buffer = make([]byte, 1024)
	initMsg := initialMessage{}
	if err := c.processCommand(conn, "", &initMsg); err != nil {
		return fmt.Errorf("initial message error: %w", err)
	}

	c.log.V(4).Info("connection established",
		"version", initMsg.Version,
		"pid", initMsg.PID,
		"max_output_len", initMsg.MaxOutputLength,
	)
	c.buffer = make([]byte, initMsg.MaxOutputLength)

	return nil
}

// handleDiscovery determines which of the watched CPUs are DPDK lcores and
// which of those report usage, and records a CPUClass for every watched CPU.
// It also builds the lcore ID to CPU ID mapping that handleUsage relies on.
func (c *dpdkTelemetryConnection) handleDiscovery(conn net.Conn) error {
	if testHookDiscoveryLoop != nil {
		return testHookDiscoveryLoop()
	}

	var list lcoreListResponse
	if err := c.processCommand(conn, lcoreListCommand, &list); err != nil {
		return fmt.Errorf("lcore list error: %w", err)
	}

	// DPDK below 23.03 does not expose the lcore endpoints and answers with a
	// null body. The usage endpoint arrived with them, so such an application
	// can report neither its layout nor its load: nothing about it can be
	// scaled, and asking for polling-dpdk scaling is a configuration error.
	if len(list.LcoreIDs) == 0 {
		if !c.loggedNoLcoreAPI {
			c.log.Error(ErrDPDKNoLcoreEndpoints,
				"dpdk frequency scaling is not supported by this application, "+
					"its CPUs are held at the application frequency")
			c.loggedNoLcoreAPI = true
		}
		c.lcoreIDByCPU = map[uint]uint{}
		c.classify(CPUClassUnsupported)
		return nil
	}

	lcoreIDByCPU := map[uint]uint{}
	// CPUs running more than one lcore. Tracked separately because they drop
	// out of lcoreIDByCPU yet are known to be running DPDK work.
	sharedCPUs := map[uint]struct{}{}
	var unpinnedLcores []uint
	for _, lcoreID := range list.LcoreIDs {
		var res lcoreInfoResponse
		if err := c.processCommand(conn, fmt.Sprintf(lcoreInfoCommand, lcoreID), &res); err != nil {
			return fmt.Errorf("lcore info error: %w", err)
		}
		// An lcore EAL has disabled is skipped by rte_lcore_iterate, so it
		// reaches neither endpoint: it is absent from the list, and asking for
		// its info yields an empty body that unmarshals to a zero value. An
		// empty cpuset is the only signal there is, and the only one needed.
		if len(res.Info.CPUSet) == 0 {
			continue
		}
		// An lcore free to move between CPUs has usage that belongs to none of
		// them, and skipping it would not remove its load from the CPUs it can
		// run on: a pinned lcore sharing one of those CPUs would then be scaled
		// on usage that accounts for only part of the work there, and a CPU of
		// the cpuset with no lcore of its own could be driven to the hardware
		// minimum underneath it. Scaling needs lcores pinned one to a CPU, so
		// one unpinned lcore makes the whole container unscalable. Collect them
		// all so the operator can fix them in one pass.
		if len(res.Info.CPUSet) > 1 {
			unpinnedLcores = append(unpinnedLcores, lcoreID)
			continue
		}

		// Mirror image: several lcores pinned to one CPU. The usage of any one
		// of them describes only part of what the CPU is doing, so scaling on
		// it would read the CPU as idler than it is. Drop the CPU rather than
		// let the last lcore of the list silently win.
		cpuID := res.Info.CPUSet[0]
		if _, taken := lcoreIDByCPU[cpuID]; taken {
			delete(lcoreIDByCPU, cpuID)
			sharedCPUs[cpuID] = struct{}{}
		}
		if _, shared := sharedCPUs[cpuID]; shared {
			if _, logged := c.loggedSharedCPU[cpuID]; !logged {
				c.log.Info("cpu runs more than one dpdk lcore and will not be scaled", "cpu", cpuID)
				c.loggedSharedCPU[cpuID] = struct{}{}
			}
			continue
		}
		lcoreIDByCPU[cpuID] = lcoreID
	}

	if len(unpinnedLcores) > 0 {
		if !c.loggedUnpinned {
			c.log.Error(ErrDPDKUnpinnedLcore,
				"dpdk frequency scaling is not supported by this application, "+
					"its CPUs are held at the application frequency",
				"lcores", unpinnedLcores)
			c.loggedUnpinned = true
		}
		c.lcoreIDByCPU = map[uint]uint{}
		c.classify(CPUClassUnsupported)
		return nil
	}
	c.lcoreIDByCPU = lcoreIDByCPU

	var usage usageResponse
	if err := c.processCommand(conn, usageCommand, &usage); err != nil {
		return fmt.Errorf("usage error: %w", err)
	}
	for _, lcoreID := range usage.Usage.LcoreIDs {
		c.pollingLcores[lcoreID] = struct{}{}
	}

	// The lcore endpoints are present, but reporting usage is opt-in for the
	// application. Without it every lcore stays silent, so there is nothing to
	// scale and asking for polling-dpdk scaling is a configuration error.
	if len(c.pollingLcores) == 0 && !c.loggedNoLcoreUsage {
		c.log.Error(ErrDPDKNoLcoreUsage,
			"dpdk lcores cannot be scaled, they are held at the application frequency",
			"lcores", len(list.LcoreIDs))
		c.loggedNoLcoreUsage = true
	}

	for _, cpuID := range c.watchedCPUs {
		class := CPUClassNonLcore
		if lcoreID, isLcore := lcoreIDByCPU[cpuID]; isLcore {
			class = CPUClassSilentLcore
			if _, polling := c.pollingLcores[lcoreID]; polling {
				class = CPUClassPollingLcore
			}
		} else if _, shared := sharedCPUs[cpuID]; shared {
			// Running lcores, just not scalable ones. Treating it as a CPU
			// with no lcore on it would let an lcore sibling drive it down to
			// the hardware minimum while DPDK work is on it.
			class = CPUClassSilentLcore
		}
		c.storeClass(cpuID, class)
	}

	return nil
}

// classify applies one class to every watched CPU.
func (c *dpdkTelemetryConnection) classify(class CPUClass) {
	for _, cpuID := range c.watchedCPUs {
		c.storeClass(cpuID, class)
	}
}

// storeClass records a CPU's class, logging it only when it changes.
func (c *dpdkTelemetryConnection) storeClass(cpuID uint, class CPUClass) {
	if prev, found := c.cpuClasses.Swap(cpuID, class); !found || prev.(CPUClass) != class {
		c.log.V(4).Info("classified cpu", "cpu", cpuID, "class", class)
	}
}

// handleUsage polls the DPDK usage endpoint, computes per-CPU windowed usage
// (percent over samplePeriod) from cumulative counters, and updates deltaUsage.
// If a sample is unavailable for a CPU, stores an error for that CPU.
func (c *dpdkTelemetryConnection) handleUsage(conn net.Conn) error {
	if testHookHandleMetricsLoop != nil {
		return testHookHandleMetricsLoop()
	}

	var res usageResponse
	if err := c.processCommand(conn, usageCommand, &res); err != nil {
		return fmt.Errorf("usage error: %w", err)
	}

	// The usage arrays are indexed by lcore ID, which is only the same as the
	// host CPU ID when the application happens to map them identically.
	indexByLcoreID := map[uint]int{}

	for index, lcoreID := range res.Usage.LcoreIDs {
		indexByLcoreID[lcoreID] = index
	}

	for _, cpuID := range c.watchedCPUs {
		result := telemetryResult{}

		if index, found := indexByLcoreID[c.lcoreIDByCPU[cpuID]]; found && c.isLcoreCPU(cpuID) {
			currTotal := res.Usage.TotalCycles[index]
			currBusy := res.Usage.BusyCycles[index]

			// Compute delta between consecutive cumulative counters.
			if prev, ok := c.prevUsageCycles.Load(cpuID); ok {
				prevSnap := prev.(cycleCounters)
				if currTotal >= prevSnap.total && currBusy >= prevSnap.busy {
					deltaTotal := currTotal - prevSnap.total
					deltaBusy := currBusy - prevSnap.busy

					if deltaTotal == 0 {
						result.percent = 0
					} else {
						percent := (deltaBusy * 100) / deltaTotal
						// Inconsistent sample. Skip this tick.
						if percent > 100 {
							result.err = ErrDPDKMetricNotProvided
						}
						result.percent = int(percent)
					}
				} else {
					// Counters reset or went backwards. Skip this tick.
					result.err = ErrDPDKMetricNotProvided
				}
			} else {
				// First observation for this CPU. Need a baseline before reporting.
				result.err = ErrDPDKMetricNotProvided
			}

			// Store current as previous for the next iteration.
			c.prevUsageCycles.Store(cpuID, cycleCounters{total: currTotal, busy: currBusy})
		} else {
			result.err = ErrDPDKMetricNotProvided
		}
		c.deltaUsage.Store(cpuID, result)
	}

	return nil
}

// isLcoreCPU reports whether a CPU has a DPDK lcore pinned to it.
func (c *dpdkTelemetryConnection) isLcoreCPU(cpuID uint) bool {
	_, found := c.lcoreIDByCPU[cpuID]
	return found
}

func (c *dpdkTelemetryConnection) clearMetrics(cpuList []uint) {
	for _, cpuID := range cpuList {
		c.deltaUsage.Delete(cpuID)
		c.prevUsageCycles.Delete(cpuID)
		c.cpuClasses.Delete(cpuID)
	}
}
