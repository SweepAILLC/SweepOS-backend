#!/usr/bin/env python3
"""
Check for potential routing conflicts in FastAPI routers.
This script helps prevent duplicate endpoint paths that could cause routing issues.
"""
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Read main.py to get router registration order
main_py_path = Path(__file__).parent.parent / "app" / "main.py"
with open(main_py_path) as f:
    main_content = f.read()

# Extract router registrations
router_pattern = r'app\.include_router\((\w+)\.router,\s*prefix="([^"]+)"'
routers = re.findall(router_pattern, main_content)

print("=" * 60)
print("Router Registration Order")
print("=" * 60)
for i, (router_name, prefix) in enumerate(routers, 1):
    print(f"{i}. {router_name}.router → prefix='{prefix}'")
print()

# Find all endpoint definitions
api_dir = Path(__file__).parent.parent / "app" / "api"
endpoints_by_path = defaultdict(list)

for py_file in api_dir.glob("*.py"):
    if py_file.name.startswith("__"):
        continue
    
    with open(py_file) as f:
        content = f.read()
    
    # Find router decorators
    pattern = r'@router\.(get|post|put|patch|delete)\(["\']([^"\']+)["\']'
    matches = re.findall(pattern, content)
    
    for method, path in matches:
        endpoints_by_path[path].append({
            "file": py_file.name,
            "method": method,
            "path": path
        })

# Check for conflicts
print("=" * 60)
print("Potential Routing Conflicts")
print("=" * 60)

conflicts_found = False
for path, endpoints in endpoints_by_path.items():
    if len(endpoints) > 1:
        conflicts_found = True
        print(f"\n⚠️  CONFLICT: Path '{path}' defined in multiple files:")
        for endpoint in endpoints:
            print(f"   - {endpoint['method'].upper()} in {endpoint['file']}")

if not conflicts_found:
    print("✅ No routing conflicts found!")

print()
print("=" * 60)
print("All Endpoints by Router")
print("=" * 60)

# Group by router file
endpoints_by_file = defaultdict(list)
for path, endpoints in endpoints_by_path.items():
    for endpoint in endpoints:
        endpoints_by_file[endpoint['file']].append(endpoint)

for router_name, prefix in routers:
    # Find corresponding file
    file_name = f"{router_name}.py"
    if file_name in endpoints_by_file:
        print(f"\n{router_name}.router (prefix='{prefix}'):")
        for endpoint in sorted(endpoints_by_file[file_name], key=lambda x: x['path']):
            full_path = f"{prefix}{endpoint['path']}"
            print(f"  {endpoint['method'].upper():6} {full_path}")

print()
print("=" * 60)

