# CephFS Architecture & Troubleshooting

## Understanding CephFS Components

When your mars_worker nodes mount CephFS, they interact with three different types of services:

### 1. **Monitors (MON)** - Entry Points (Port 6789)
```bash
# Example mount string:
10.106.20.40:6789,10.106.20.41:6789:/shared/path
```
- **What they do**: Maintain cluster maps, authenticate clients, provide initial connection
- **When you use them**: Only during mount and for cluster state queries
- **You control**: YES - these IPs are in your /etc/fstab mount string
- **Can remove problematic ones**: YES - if a monitor is down, remove it from mount string

### 2. **Metadata Servers (MDS)** - File Operations (Ports 6800+)
```bash
# Example from dmesg:
mds0 10.106.20.40:6804 connection timeout
```
- **What they do**: Handle file metadata (open, stat, readdir, permissions)
- **When you use them**: Every time you access file/directory metadata
- **You control**: NO - cluster assigns MDS automatically
- **Can change via mount**: NO - cluster decides which MDS you talk to

### 3. **Object Storage Daemons (OSD)** - Actual Data (Ports 6800-7000+)
```bash
# Example from dmesg:
osd583 (1)10.106.20.41:6971 socket closed (con state V1_BANNER)
osd575 (1)10.106.20.41:6907 socket closed (con state V1_BANNER)
```
- **What they do**: Store actual file data in chunks (objects)
- **When you use them**: During read/write/fsync operations
- **You control**: NO - cluster assigns OSDs based on CRUSH map and data placement
- **Can change via mount**: NO - cluster decides which OSDs store your data

## Why Your Mount IPs ≠ Error IPs

**Your mount string:**
```
10.106.20.40:6789,10.106.20.41:6789:/shared/path
```

**Errors you see:**
```
[Mon Feb  9 19:49:02 2026] libceph: osd575 (1)10.106.20.41:6907 socket closed
[Mon Feb  9 19:49:06 2026] libceph: osd572 (1)10.106.20.41:6931 socket closed
```

**Why different ports?**
- Mount monitors use port **6789** (fixed)
- OSDs use dynamic ports **6800-7000+** (assigned by cluster)
- Same IP (10.106.20.41) can host BOTH monitors AND OSDs

**The problem:**
- The **OSDs on 10.106.20.41** are having issues (ports 6907, 6931, 6971, etc.)
- The **monitor on 10.106.20.41:6789** might be fine
- Removing the monitor won't help because the problem is with OSDs, not monitors

## What Causes Slow fsync()?

When you call `fsync()` on a CephFS file:

1. **Client → OSDs**: Flush dirty data to OSDs
2. **Wait for acknowledgment**: All replica OSDs must confirm write
3. **Update metadata**: Notify MDS that data is committed

**When OSDs have connection issues:**
```
Storage node problems → OSD socket close → Client detects failure → 
→ TCP timeout (100-500ms) → Reconnect attempt → 
→ Or failover to replica OSD → Eventually completes
```

This is why you see fsync times varying from **5ms to 800ms**:
- **5-50ms**: Normal operation, all OSDs healthy
- **100-300ms**: One OSD reconnecting, client waiting for timeout
- **500-800ms**: Multiple OSD failures, multiple reconnects/failovers

## Diagnostic Commands

### Check Current Mount
```bash
mount | grep "type ceph"
# Shows: 10.106.20.40:6789,10.106.20.41:6789:/shared on /mnt/shared type ceph
```

### Monitor Live CephFS Errors
```bash
dmesg -T -w | grep -i ceph
# Watch for OSD/MDS connection issues in real-time
```

### See Recent Issues
```bash
dmesg -T | grep -i ceph | tail -50
# Last 50 CephFS-related kernel messages
```

### Run Health Check
```bash
source /opt/cads_mars_server_venv/bin/activate
check-cephfs-health
# Comprehensive analysis with explanations
```

### Check Specific OSD Status (if you have ceph client)
```bash
ceph osd tree | grep -i down
ceph health detail
```

## Troubleshooting

### ❌ Won't Help
- **Removing monitor 10.106.20.41:6789 from mount string**
  - The errors are from OSDs, not monitors
  - You'd lose redundancy without fixing the root cause

### ✅ Will Help
1. **Contact storage team with**:
   - Failing OSD numbers (osd575, osd572, osd583, etc.)
   - Output of `dmesg -T | grep ceph | tail -100`
   - Output of `check-cephfs-health` command
   
2. **Storage team should check**:
   - Why those specific OSDs are dropping connections
   - Network issues on storage node 10.106.20.41
   - OSD daemon health/logs on that node
   - Storage node hardware (disk, memory, network)

3. **Temporary mitigation** (if storage team can't help immediately):
   - Accept the slow fsync times (100-800ms)
   - Monitor for complete failures (fsync errors, not just slow)
   - Consider reducing concurrent requests to reduce load

## Example: Good vs. Bad Logs

### ✅ Healthy CephFS
```
[INFO] Starting sync of output file /shared/test.grib (size: 245.32 MB)
[INFO] Output file successfully synced to CephFS in 0.042 seconds
```

### ⚠️ Degraded Performance (Still Working)
```
[INFO] Starting sync of output file /shared/test.grib (size: 245.32 MB)
[WARN] ⚠️  SLOW FSYNC: 0.847s for /shared/test.grib (245.32 MB)
       Possible CephFS storage backend issue. This typically indicates
       OSD connection problems (reconnects/failovers).
```

### ❌ Complete Failure (Needs Urgent Action)
```
[ERROR] Failed to sync output file: [Errno 5] Input/output error
[ERROR] CephFS mount point /shared became unresponsive
```

## Summary

**What you CAN control:**
- Monitor IPs in mount string (e.g., `10.106.20.40:6789`)

**What you CANNOT control:**
- Which OSDs store your data
- Which MDS handles your metadata
- OSD connection stability (storage backend issue)

**When you see OSD errors:**
- ✅ Report to storage team
- ✅ Include OSD numbers and IPs
- ✅ Include dmesg output
- ❌ Don't modify mount string (won't help)
- ❌ Don't add more monitors (not the problem)

**The root cause:** Storage infrastructure issues on specific nodes hosting OSDs, not your client configuration.
