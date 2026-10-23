# Dynamic CPU Frequency Scaling for DPDK Polling Workloads

## Overview

- This feature dynamically adjusts CPU frequency for DPDK polling applications based on the
[DPDK Telemetry](https://doc.dpdk.org/guides/howto/telemetry.html) `/eal/lcore/usage` metric.
The Cluster Power Manager (CPM) samples per‑CPU usage exposed by the DPDK telemetry socket,
computes a windowed utilization for each CPU, and steers the CPU frequency to keep that
utilization near a target band.
- Actuation is done through the Linux cpufreq subsystem using the `userspace` governor, which
allows CPM to set explicit target frequencies.
- To use this feature with a DPDK application, the application must register a callback for a
standard DPDK telemetry endpoint (/eal/lcore/usage). An
[example callback](https://github.com/DPDK/dpdk/blob/95e85ea4d65d74655217166012896fc5d0b40a4e/app/test-pmd/testpmd.c#L2383)
can be found in the DPDK test-pmd application.

## How it works

### Configuration parameters

Scaling behavior is configured in the `PowerProfile` under `spec.cpuScalingPolicy`. For example:

```yaml
apiVersion: power.cluster-power-manager.github.io/v1alpha1
kind: PowerProfile
metadata:
  name: dpdk-scaling
spec:
  pstates:
    governor: userspace
    min: 0%
    max: 100%
  cpuScalingPolicy:
    workloadType: polling-dpdk
    samplePeriod: 20ms
    cooldownPeriod: 60ms
    targetUsage: 80
    allowedUsageDifference: 5
    allowedFrequencyDifference: 10
    scalePercentage: 100
    fallbackFreqPercent: 100
    appFreqPercent: 80
```

- `workloadType`: must be `polling-dpdk` to enable this feature for a profile.
- `samplePeriod`: interval at which the agent samples usage and decides whether to adjust CPU frequency.
- `cooldownPeriod`: waiting time after a frequency change before another adjustment for the same CPU is considered.
- `targetUsage`: desired usage percentage for each managed CPU.
- `allowedUsageDifference`: deadband around the target; when usage is within this band, the scaler holds the current target.
- `allowedFrequencyDifference`: minimum step (MHz) required to actually apply a computed change.
- `scalePercentage`: proportional gain (10–200). Higher values react more aggressively to error.
- `fallbackFreqPercent`: target frequency for a DPDK lcore while no usage sample is available.
This is a transient value, used at startup, after a reconnect, and whenever a sample cannot be
used. It defaults to 100 because a polling lcore that is not being measured must be assumed busy;
lowering it trades packet loss during those gaps for power.
- `appFreqPercent`: target frequency for the CPUs of the container that are not DPDK polling
lcores. See [CPU classification](#cpu-classification). It defaults to 80, giving work CPM cannot
measure most of the frequency range while leaving out the least efficient part of it; raise it if
those CPUs are latency sensitive.

Both frequency percentages are a position in the CPU's hardware frequency range, not a fraction
of its maximum: 0 is the hardware minimum, 100 the hardware maximum, and the value in between is

```
frequency = min + (max - min) * percent / 100
```

On a CPU running from 800 MHz to 3700 MHz, `appFreqPercent: 80` is therefore 3120 MHz, not
2960 MHz.

### CPU classification

A container's cpuset is rarely all polling lcores. CPM asks DPDK telemetry which of the CPUs it
manages are lcores, and which of those report usage, and drives each class differently:

| Class | How it is detected | Frequency |
| --- | --- | --- |
| Polling lcore | listed by `/eal/lcore/list`, and its lcore ID appears in `/eal/lcore/usage` | dynamically scaled |
| Polling lcore with no usable sample | as above, but the current sample cannot be used | `fallbackFreqPercent` |
| Silent lcore | it runs lcores, but none that can be scaled: its lcore never appears in `/eal/lcore/usage` — typically the main lcore — or it runs several lcores at once | `appFreqPercent` |
| Non-lcore sharing a core with an lcore | no lcore is pinned to it, but one is pinned to a CPU of the same physical core | hardware minimum |
| Non-lcore | no lcore is pinned to it, and none to any CPU of its physical core | `appFreqPercent` |
| Unsupported | the application as a whole cannot be scaled — see [Application requirements](#application-requirements) | `appFreqPercent` |
| Not yet classified | telemetry is not connected yet | `fallbackFreqPercent` |

Scaling an lcore needs a one-to-one match between it and a CPU, so that the usage it reports
describes everything that CPU is doing. Two layouts break that, and they break it to different
degrees:

- **An lcore pinned to more than one CPU** is free to move between them, so its usage belongs to
  no single CPU — and skipping it would not remove its load from the CPUs it can run on. A
  pinned lcore sharing one of those CPUs would then be scaled on usage accounting for only part
  of the work there, and a CPU of the cpuset with no lcore of its own would look like a plain
  non-lcore and could be driven to the hardware minimum under a running lcore. Neither can be
  reasoned about locally, so a single unpinned lcore makes the whole container unscalable.
- **Several lcores pinned to the same CPU** each account for only part of that CPU's work, so
  scaling on any one of them would read the CPU as idler than it is. This one is contained: the
  CPU is treated as a silent lcore, since it is running DPDK work even though none of it can be
  sized, and the container's other CPUs are unaffected.

This matters most for the fourth row. A physical core has a single frequency, shared by its
Hyper-Threading siblings, so a sibling asking for a high frequency puts a hard floor under the
core. Its lcore sibling would keep computing and writing lower targets that have no physical
effect, and the power saving at low load would be silently lost. Holding such a sibling at the
hardware minimum leaves the lcore free to drive the core's frequency.

A silent lcore is held at `appFreqPercent` whatever else is on its physical core, since it is
running application work that CPM cannot size. Placing one on the Hyper-Threading sibling of a
polling lcore therefore floors that core at `appFreqPercent` and stops the polling lcore from
scaling down. Pin the main lcore to a physical core of its own, or lower `appFreqPercent`.

Two polling lcores may share a physical core. Each is scaled from its own usage against its own
frequency target, and the core settles at the higher of the two, so neither constrains the other.

### Application requirements

Dynamic scaling asks three things of the application:

- The `/eal/lcore/list`, `/eal/lcore/info` and `/eal/lcore/usage` telemetry endpoints, all added
  in DPDK 23.03.
- Per-lcore usage, reported by registering a callback with `rte_lcore_register_usage_cb()`.
  This is opt-in, so a recent DPDK is not on its own enough.
- Every lcore pinned to a single CPU, for example with `-l` or an explicit `--lcores` mapping.

Requesting `workloadType: polling-dpdk` for an application that does not meet all three is a
configuration error. The node agent logs it once per telemetry connection.
Nothing is scaled dynamically and no power is saved, until the application is fixed or
the profile stops requesting `polling-dpdk`.

### Frequency adjustment flow

1. A guaranteed pod requesting a `PowerProfile` with `workloadType: polling-dpdk` is reconciled by the PowerPod controller.
2. The controller ensures a telemetry worker is created per DPDK pod to connect to its telemetry socket and sample cumulative `busy/total` cycle counters at a fixed internal window (≈10 ms). The worker computes windowed usage per CPU and stores the latest sample in a thread‑safe map. On the same connection, it rediscovers the application's lcore layout every second and records a class for each managed CPU.
3. For each managed CPU that is a polling lcore, a scaling worker evaluates control every `samplePeriod`. CPUs of any other class are held at the fixed frequency their class calls for:
   - Reads the latest windowed usage.
   - If usage is within `targetUsage ± allowedUsageDifference`, hold the current target.
   - Otherwise, compute a new target with the proportional rule below, clamp to limits, and apply if it exceeds `allowedFrequencyDifference`.
   - After applying, wait `cooldownPeriod` before evaluating that CPU again.

#### Windowed usage computation

The `/eal/lcore/usage` endpoint exposes cumulative counters. To base decisions on recent activity, the telemetry worker derives a per‑CPU windowed usage over its fixed sampling window.

`total_cycles[i]` and `busy_cycles[i]` are cumulative. On each poll, compute deltas against the previous sample:

```console
delta_total = total_cycles[i]_now - total_cycles[i]_prev
delta_busy  = busy_cycles[i]_now  - busy_cycles[i]_prev
delta_usage% = 100 * delta_busy / delta_total   # clamped to [0, 100]
```

#### Frequency update formula

```console
nextTargetFrequency = currentFrequency * (1 + (currentUsage / targetUsage - 1) * scalePercentage)
```

Then apply filters and limits:

- If `|nextTargetFrequency - previousTarget| < allowedFrequencyDifference`, hold the previous target.
- If usage is within the allowed range, hold the previous target.
- Clamp to configured min/max (`pstates.min/max`).
- Set the new target via cpufreq `userspace`, then wait `cooldownPeriod` before reevaluating that CPU.

## Testing and monitoring

1. Apply a PowerProfile with a scaling policy (see example above) to the cluster where you will run the DPDK app.

2. Launch the example DPDK server/client pair:

    ```console
    KUBE_CLI=kubectl ./testbin/dpdk-testapp.sh [--non-siblings] [--replicas N]
    ```

    - The script uses `kubectl` by default. Set `KUBE_CLI=oc` to use the OpenShift CLI instead.
    - The script deploys [examples/example-dpdk-testapp.yaml](../examples/example-dpdk-testapp.yaml) and starts two `dpdk-testpmd` instances in each pod: a client (traffic generator) and a server (receiver/forwarder). The scaler manages the CPUs assigned to the server container.
    - If Hyper-Threading is enabled, pass `--non-siblings` to pin the server to one logical CPU per physical core. If HT is disabled, omit the flag.
    - To launch multiple DPDK pods, pass `--replicas N` (default: 1). Each pod gets its own DPDK telemetry connection and CPU scaling.

3. Monitor frequency and usage on the node with [cpmon.py](../testbin/cpmon.py). Copy the script to the target node and run it there, as it reads CPU sysfs and MSR data locally:

    ```sh
    # On the target node:
    export KUBECONFIG=<path-to-kubeconfig>
    KUBE_CLI=kubectl  # set KUBE_CLI=oc on OpenShift
    NODE=$(hostname)
    POD="<pod-name>"
    CPUS=$("$KUBE_CLI" get powernodestate "$NODE-power-state" -n power-manager -o jsonpath="{range .status.cpuPools.exclusive[?(@.pod==\"$POD\")]}{range .powerContainers[?(@.name==\"server\")]}{.cpuIDs}{end}{end}" | tr -d '[]')
    PODID=$("$KUBE_CLI" get powernodestate "$NODE-power-state" -n power-manager -o jsonpath="{range .status.cpuPools.exclusive[?(@.pod==\"$POD\")]}{.podUID}{end}")
    ./cpmon.py --cpu "$CPUS" --dpdk-pod-uid "$PODID" [--no-siblings] [--scroll]
    ```

4. To tear down the DPDK test app:

    ```console
    KUBE_CLI=kubectl ./testbin/dpdk-testapp.sh -d
    ```
