# mars-daemon: DaemonSet-based MARS Client for Kubernetes

## Problem Statement

The MARS client protocol requires the caller to **open an ephemeral TCP port** and wait for remote "movers" to push data to it.  In Kubernetes this is problematic:

- Pods have **dynamic IPs** that movers outside the cluster may not be able to reach.
- NetworkPolicy and CNI rules often **block inbound connections** to arbitrary pod ports.
- Services expose fixed ports — not the ephemeral ports MARS negotiates at runtime.

### Current Workaround

`cads-mars-server` solves this by running a **persistent HTTP/WebSocket proxy** (Deployment) that:

1. Listens on a known port (9000/9001).
2. Forks + execs the real `mars` binary on behalf of the caller.
3. Streams data back to the caller over the already-established outbound connection.

This works, but adds:

- **Network hops**: worker → mars-server → movers → mars-server → worker.
- **Serialisation bottleneck**: every byte passes through the server process.
- **Scaling complexity**: the server Deployment needs to be sized for aggregate throughput.

---

## Proposed Architecture

Replace the centralised server with a **DaemonSet** that runs on every worker node.

```
┌──────────────── Node ────────────────────┐
│                                          │
│  ┌─────────────┐    ┌────────────────┐   │
│  │ Worker Pod   │    │ mars-daemon    │   │
│  │ (task runner)│    │ (DaemonSet)    │   │
│  │              │    │                │   │
│  │ PVC ──────────────── same PVC*    │   │
│  │  /data       │    │  /data         │   │
│  │              │    │                │   │
│  │  POST ───────────→ :9090          │   │
│  │  localhost    │    │ runs mars     │   │
│  │              │    │ writes to /data│   │
│  └─────────────┘    └────────────────┘   │
│                                          │
│  * Mounted via hostPath or projected PVC │
└──────────────────────────────────────────┘
```

### Key Properties

| Property | DaemonSet advantage |
|----------|-------------------|
| **Network** | `hostNetwork: true` — movers connect to the **node IP** (stable, routable). |
| **Filesystem** | Shares the worker's PVC via projected volume or hostPath — **zero network copy** for the output file. |
| **Locality** | Always co-located on the same node — communication via `localhost` or Unix socket. |
| **Scaling** | One daemon per node — scales with the cluster automatically. |

---

## Data Flow

### Current flow (mars-server Deployment)

```
Worker Pod ──HTTP POST──▶ mars-server Pod ──fork/exec──▶ mars binary
                                                           │
                                        ◀──pipe/stream─────┘
              ◀──HTTP chunked──────────┘
Worker Pod writes to local file
```

Every byte crosses the network twice (worker→server, mars→server→worker).

### Proposed flow (mars-daemon DaemonSet)

```
Worker Pod ──HTTP POST──▶ mars-daemon (localhost)──fork/exec──▶ mars binary
                                                                  │
                                                  writes to ──────┘
                                                  shared PVC
Worker Pod reads from shared PVC (same filesystem, zero copy)
```

Only the small JSON request crosses localhost.  MARS writes directly to the worker's storage.

---

## Shared Storage Strategy

The worker pod's storage should be accessible to the daemon.  Three approaches, from simplest to most flexible:

### Option A: hostPath projection (simplest)

The daemon mounts a well-known hostPath directory (e.g. `/var/mars-data/`).  Worker pods mount the same hostPath.

```yaml
# Worker pod
volumes:
  - name: mars-data
    hostPath:
      path: /var/mars-data
      type: DirectoryOrCreate

# DaemonSet
volumes:
  - name: mars-data
    hostPath:
      path: /var/mars-data
      type: DirectoryOrCreate
```

**Pros**: No special permissions.  Works with any CNI.  
**Cons**: Data is node-local — not durable.  Only suitable when the worker consumes data immediately.

### Option B: CephFS / shared PVC (current infra)

Both the daemon and the worker mount the same CephFS-backed PVC (ReadWriteMany).

```yaml
volumes:
  - name: shared-cephfs
    persistentVolumeClaim:
      claimName: mars-shared
```

**Pros**: Durable, survives pod restarts.  Already in use today.  
**Cons**: Performance depends on CephFS OSD health (see `docs/CEPHFS_ARCHITECTURE.md`).

### Option C: Dynamic PVC projection (most flexible)

The worker pod tells the daemon which PVC to use.  The daemon uses the **Kubernetes API** to inspect the worker pod's volume mounts and bind-mounts the same path from the host filesystem.

This requires:
- `hostPID: true` (to see the worker pod's mount namespace)
- RBAC to read Pod specs
- `nsenter` or `mountPropagation: Bidirectional`

**Pros**: No shared convention — any PVC type works (Ceph block, local SSD, NFS).  
**Cons**: Higher privilege.  More complex implementation.

### Recommended: Start with Option A or B, design the API so Option C is a drop-in replacement.

---

## Daemon API

A minimal HTTP API on `localhost:9090` (or Unix socket `/var/run/mars-daemon.sock`):

### `POST /execute`

Submit a MARS request.  The daemon runs `mars` and writes output to the specified target path.

```json
{
  "request": {
    "class": "od",
    "type": "an",
    "date": "20240101",
    "param": "2t"
  },
  "environ": {
    "request_id": "550e8400-e29b-41d4-a716-446655440000"
  },
  "target": "/data/output.grib"
}
```

Response (streaming, line-delimited JSON):

```json
{"type": "log", "line": "MARS - INFO ..."}
{"type": "log", "line": "MARS - INFO ..."}
{"type": "state", "status": "finished", "exit_code": 0, "bytes": 104857600}
```

Or on error:

```json
{"type": "state", "status": "error", "exit_code": 1, "error": "No matching data"}
```

### `HEAD /`

Health check — returns `204` if the daemon is running.

### `GET /{uid}`

Retrieve the MARS log for a completed request.

### `DELETE /{uid}`

Clean up logs for a completed request.

This API is intentionally compatible with `cads-mars-server` endpoints so clients can switch backends with a URL change.

---

## DaemonSet Manifest (skeleton)

```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: mars-daemon
  namespace: mars
spec:
  selector:
    matchLabels:
      app: mars-daemon
  template:
    metadata:
      labels:
        app: mars-daemon
    spec:
      hostNetwork: true          # Movers can reach node IP
      dnsPolicy: ClusterFirstWithHostNet
      tolerations:
        - operator: Exists       # Run on all nodes
      containers:
        - name: mars-daemon
          image: mars-daemon:latest
          ports:
            - containerPort: 9090
              hostPort: 9090
              protocol: TCP
          env:
            - name: MARS_EXECUTABLE
              value: /usr/local/bin/mars
            - name: MARS_LOGDIR
              value: /var/log/mars-daemon
            - name: NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
          volumeMounts:
            - name: mars-data
              mountPath: /data
            - name: logs
              mountPath: /var/log/mars-daemon
          readinessProbe:
            httpGet:
              path: /
              port: 9090
            initialDelaySeconds: 5
          livenessProbe:
            httpGet:
              path: /
              port: 9090
            periodSeconds: 30
          resources:
            requests:
              cpu: 100m
              memory: 256Mi
            limits:
              cpu: "2"
              memory: 2Gi
      volumes:
        - name: mars-data
          hostPath:
            path: /var/mars-data
            type: DirectoryOrCreate
        - name: logs
          emptyDir: {}
```

---

## Worker Pod Integration

### Minimal change to existing client code

```python
import os
from cads_mars_server.client import RemoteMarsClient

# Instead of pointing at the centralised server:
#   client = RemoteMarsClient(url="http://mars-server:9000")
# Point at localhost (the DaemonSet on this node):
client = RemoteMarsClient(url="http://127.0.0.1:9090")

result = client.execute(
    request={"class": "od", "type": "an"},
    environ={"request_id": uid},
    target="/data/output.grib",   # Shared mount
)
```

With `hostNetwork` the daemon is always at `127.0.0.1:9090` from any pod on the same node — no service discovery needed.

### Fallback

If the DaemonSet is down (node drain, upgrade), the worker can fall back to the centralised `mars-server` Deployment.  `RemoteMarsClientCluster` already supports this:

```python
cluster = RemoteMarsClientCluster(
    urls=["http://127.0.0.1:9090", "http://mars-server:9000"],
    retries=3,
)
```

---

## Open Questions

1. **Concurrency**: How many parallel MARS processes per node?  Should the daemon enforce a limit (like `MAX_CONCURRENT_CONNECTIONS` in `ws_server.py`)?

2. **Cleanup**: Who removes old output files?  The worker (after consuming), or the daemon (after a TTL)?

3. **Mover reachability**: Does `hostNetwork: true` suffice, or do movers need explicit routes to the Kubernetes node subnet?  This depends on the MARS infrastructure network topology.

4. **Security**: The daemon runs with elevated privileges (`hostNetwork`, possibly `hostPID`).  RBAC and PodSecurityPolicy / PodSecurityStandard need careful scoping.

5. **Block storage PVCs**: Ceph block (RBD) PVCs are `ReadWriteOnce` — they can't be mounted by two pods simultaneously.  For Option C (dynamic PVC projection), the daemon would need to access the mount via the host's filesystem (`/var/lib/kubelet/pods/<pod-uid>/volumes/...`) rather than mounting the PVC itself.

6. **Observability**: Should the daemon expose Prometheus metrics (request count, latency, MARS exit codes, CephFS fsync times)?

---

## Implementation Phases

### Phase 1: Proof of Concept
- [ ] Minimal Python daemon reusing `server.py` / `server_read_and_stream.py` internals
- [ ] hostPath shared volume (Option A)
- [ ] DaemonSet manifest
- [ ] Test with fake_mars on a local k8s cluster (kind/minikube)

### Phase 2: Production Hardening
- [ ] CephFS shared PVC (Option B)
- [ ] Concurrency limits + request queuing
- [ ] Graceful shutdown (drain in-flight requests on SIGTERM)
- [ ] Prometheus metrics endpoint
- [ ] Helm chart or Kustomize overlay

### Phase 3: Dynamic Volume Projection
- [ ] Kubernetes API integration for pod volume inspection (Option C)
- [ ] Mount propagation or nsenter-based access
- [ ] RBAC manifests for pod read access
- [ ] Support for RWO block storage PVCs
