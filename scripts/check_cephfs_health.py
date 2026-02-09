#!/usr/bin/env python3
"""
Check CephFS health and suggest mount string modifications.

Run this on mars_worker nodes to diagnose CephFS/MDS issues.

=== CephFS Architecture Overview ===

CephFS has three main components that your client connects to:

1. **Monitors (MON)** - Specified in your mount string
   Example: 10.106.20.40:6789,10.106.20.41:6789:/path
   - Entry points to the cluster
   - Maintain cluster state and maps
   - Tell clients where to find MDS and OSDs
   - These are the IPs you CAN control in your mount string

2. **Metadata Servers (MDS)** - Assigned automatically by cluster
   - Handle file/directory metadata operations (open, stat, ls, etc.)
   - Client connects to these automatically after mounting
   - You CANNOT choose which MDS to use (cluster decides)
   - Port range typically 6800+

3. **Object Storage Daemons (OSD)** - Assigned automatically by cluster
   - Store actual file data blocks
   - Client establishes direct connections for reads/writes
   - You CANNOT choose which OSDs to use (cluster assigns based on data placement)
   - Port range typically 6800-7000+
   
**IMPORTANT**: When you see errors like "osd583 (1)10.106.20.41:6971 socket closed",
that IP:PORT is an OSD, NOT a monitor. You cannot fix OSD connection issues by
changing your mount string monitors. These errors indicate problems with:
- The storage backend cluster itself
- Network connectivity to storage nodes  
- Individual OSD daemon failures
- Storage node hardware/performance issues

The slow fsync() times you're experiencing are caused by these OSD connection issues,
as the client has to wait for reconnections or failover to replica OSDs.

**Resolution**: Contact your storage team with the specific OSD numbers/IPs that are
failing. They need to investigate why those OSDs are dropping connections.
"""
import subprocess
import re
import sys
from collections import defaultdict
from pathlib import Path


def check_mounts():
    """Find all CephFS mounts."""
    mounts = []
    try:
        with open("/proc/mounts", "r") as f:
            for line in f:
                if "type ceph" in line:
                    parts = line.split()
                    mounts.append({
                        "source": parts[0],
                        "mountpoint": parts[1],
                        "fstype": parts[2],
                        "options": parts[3]
                    })
    except Exception as e:
        print(f"Error reading mounts: {e}")
    return mounts


def check_dmesg_errors():
    """Parse recent dmesg for ceph errors."""
    osd_issues = defaultdict(int)
    mds_issues = []
    
    try:
        result = subprocess.run(
            "dmesg -T --level=err,warn | grep -i ceph | tail -100",
            shell=True,
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.stdout:
            for line in result.stdout.split('\n'):
                # Parse OSD socket closed errors
                match = re.search(r'osd(\d+).*?(\d+\.\d+\.\d+\.\d+):(\d+)', line)
                if match and "socket closed" in line.lower():
                    osd_num, ip, port = match.groups()
                    osd_issues[f"OSD {osd_num} ({ip}:{port})"] += 1
                
                # Parse MDS errors
                if "mds" in line.lower():
                    mds_issues.append(line.strip())
    
    except Exception as e:
        print(f"Error checking dmesg: {e}")
    
    return dict(osd_issues), mds_issues


def check_ceph_status():
    """Try to get ceph cluster status."""
    status = {}
    try:
        # Try ceph health
        result = subprocess.run(
            ["ceph", "health"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            status["health"] = result.stdout.strip()
        
        # Try ceph mds stat
        result = subprocess.run(
            ["ceph", "mds", "stat"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            status["mds_stat"] = result.stdout.strip()
        
        # Try ceph osd tree (to see which OSDs are down)
        result = subprocess.run(
            ["ceph", "osd", "tree"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            # Parse for down OSDs
            down_osds = []
            for line in result.stdout.split('\n'):
                if "down" in line.lower() and "osd." in line:
                    down_osds.append(line.strip())
            if down_osds:
                status["down_osds"] = down_osds
    
    except FileNotFoundError:
        print("Note: 'ceph' command not available (normal on client-only nodes)")
    except Exception as e:
        print(f"Error checking ceph status: {e}")
    
    return status


def suggest_mount_changes(mounts, osd_issues):
    """
    Analyze mount strings and OSD issues.
    Note: OSD issues cannot be fixed by changing mount monitors!
    """
    suggestions = []
    
    for mount in mounts:
        source = mount["source"]
        # Parse monitor addresses from mount source
        # Format: 10.106.20.40:6789,10.106.20.41:6789:/shared/path
        
        if osd_issues:
            # Extract monitor IPs from mount string
            mon_ips = set()
            # Match IPs followed by :6789 or similar (monitor ports)
            for match in re.finditer(r'(\d+\.\d+\.\d+\.\d+):(\d+)', source):
                mon_ips.add(match.group(1))
            
            # Extract problematic OSD IPs
            osd_ips = set()
            for osd_desc in osd_issues.keys():
                match = re.search(r'\((\d+\.\d+\.\d+\.\d+)', osd_desc)
                if match:
                    osd_ips.add(match.group(1))
            
            suggestions.append({
                "mount": mount["mountpoint"],
                "current_source": source,
                "monitor_ips": list(mon_ips),
                "osd_ips": list(osd_ips),
                "overlap": list(mon_ips & osd_ips),
                "note": (
                    "⚠️  IMPORTANT: The problematic IPs shown are OSDs (storage daemons), "
                    "not monitors. You cannot fix OSD connection issues by modifying your "
                    "mount string. These are backend storage cluster problems that require "
                    "investigation by the storage team."
                )
            })
    
    return suggestions


def main():
    print("=" * 80)
    print("CephFS Health Check")
    print("=" * 80)
    print()
    print("ℹ️  For detailed CephFS architecture explanation, see:")
    print("   docs/CEPHFS_ARCHITECTURE.md in cads-mars-server repository")
    print()
    
    # Check mounts
    print("📁 CephFS Mounts:")
    mounts = check_mounts()
    if mounts:
        for mount in mounts:
            print(f"  ✓ {mount['mountpoint']}")
            print(f"    Source: {mount['source']}")
            print(f"    Options: {mount['options']}")
    else:
        print("  ⚠️  No CephFS mounts found")
    print()
    
    # Check dmesg errors
    print("🔍 Recent CephFS Errors (from dmesg):")
    osd_issues, mds_issues = check_dmesg_errors()
    
    if osd_issues:
        print(f"  ⚠️  OSD Connection Issues ({sum(osd_issues.values())} total errors):")
        for osd, count in sorted(osd_issues.items(), key=lambda x: x[1], reverse=True):
            print(f"    • {osd}: {count} errors")
    else:
        print("  ✓ No recent OSD errors")
    print()
    
    if mds_issues:
        print(f"  ⚠️  MDS Issues ({len(mds_issues)} errors):")
        for issue in mds_issues[:10]:  # Show first 10
            print(f"    • {issue}")
        if len(mds_issues) > 10:
            print(f"    ... and {len(mds_issues) - 10} more")
    else:
        print("  ✓ No recent MDS errors")
    print()
    
    # Check ceph status
    print("📊 Ceph Cluster Status:")
    status = check_ceph_status()
    if status:
        if "health" in status:
            print(f"  Health: {status['health']}")
        if "mds_stat" in status:
            print(f"  MDS: {status['mds_stat']}")
        if "down_osds" in status:
            print(f"  ⚠️  Down OSDs:")
            for osd in status["down_osds"][:10]:
                print(f"    • {osd}")
    else:
        print("  (No cluster status available from this node)")
    print()
    
    # Suggestions
    if osd_issues or mds_issues:
        print("=" * 80)
        print("💡 Analysis & Recommendations:")
        print("=" * 80)
        suggestions = suggest_mount_changes(mounts, osd_issues)
        
        if suggestions:
            for sug in suggestions:
                print(f"\n📍 Mount Point: {sug['mount']}")
                print(f"   Current Source: {sug['current_source']}")
                if sug['monitor_ips']:
                    print(f"   Monitor IPs (in mount string): {', '.join(sug['monitor_ips'])}")
                if sug['osd_ips']:
                    print(f"   Problematic OSD IPs (from dmesg): {', '.join(sug['osd_ips'])}")
                if sug['overlap']:
                    print(f"   ⚠️  OVERLAP DETECTED: {', '.join(sug['overlap'])}")
                    print(f"      These IPs serve BOTH as monitors AND have failing OSDs")
                    print(f"      This suggests a storage node issue on these hosts")
                else:
                    print(f"   ℹ️  Monitor IPs and OSD IPs are on different hosts (normal)")
                print(f"\n   {sug['note']}")
        
        print()
        print("📋 Recommended Actions:")
        print("   1. ⚠️  Do NOT modify your mount string monitors - it won't help with OSD issues")
        print("   2. 📊 Collect this information:")
        for osd, count in list(osd_issues.items())[:5]:
            print(f"      • {osd} ({count} connection failures)")
        print("   3. 📧 Contact storage team with:")
        print("      • The failing OSD numbers and IPs listed above")
        print("      • Output of: dmesg -T | grep -i ceph | tail -50")
        print("      • This health check report")
        print("   4. 🔍 Continue monitoring: dmesg -T -w | grep ceph")
        print()
        print("❓ Why is fsync() slow?")
        print("   When OSDs disconnect, the CephFS client must:")
        print("   • Wait for TCP timeouts and reconnection attempts")
        print("   • Failover writes to replica OSDs")
        print("   • Re-establish session state with surviving OSDs")
        print("   This causes the 100-800ms fsync() delays you're seeing.")
        
        sys.exit(1)  # Exit with error if issues found
    else:
        print("✅ No CephFS issues detected!")
        sys.exit(0)


if __name__ == "__main__":
    main()
