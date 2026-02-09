#!/usr/bin/env python3
"""
Check CephFS health and suggest mount string modifications.

Run this on mars_worker nodes to diagnose CephFS/MDS issues.
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
    """Suggest modifications to mount strings based on issues."""
    suggestions = []
    
    for mount in mounts:
        source = mount["source"]
        # Parse monitor addresses from mount source
        # Format: 10.106.20.40:6789,10.106.20.41:6789:/shared/path
        if ":" in source:
            mon_part = source.split(":")[0]
            # Extract IPs that might be problematic
            problematic_ips = set()
            for osd_desc in osd_issues.keys():
                match = re.search(r'\((\d+\.\d+\.\d+\.\d+)', osd_desc)
                if match:
                    problematic_ips.add(match.group(1))
            
            if problematic_ips:
                suggestions.append({
                    "mount": mount["mountpoint"],
                    "current_source": source,
                    "problematic_ips": list(problematic_ips),
                    "note": f"Consider removing monitors at these IPs if issues persist: {', '.join(problematic_ips)}"
                })
    
    return suggestions


def main():
    print("=" * 80)
    print("CephFS Health Check")
    print("=" * 80)
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
        print("💡 Recommendations:")
        suggestions = suggest_mount_changes(mounts, osd_issues)
        
        if suggestions:
            for sug in suggestions:
                print(f"  Mount: {sug['mount']}")
                print(f"  Current: {sug['current_source']}")
                print(f"  {sug['note']}")
        
        print()
        print("  General recommendations:")
        print("  1. Check if specific monitor/MDS nodes are consistently failing")
        print("  2. Remove problematic monitors from mount string in /etc/fstab")
        print("  3. Monitor 'dmesg -T -w | grep ceph' for ongoing issues")
        print("  4. Contact storage team if issues persist")
        
        sys.exit(1)  # Exit with error if issues found
    else:
        print("✅ No CephFS issues detected!")
        sys.exit(0)


if __name__ == "__main__":
    main()
