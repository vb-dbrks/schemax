#!/usr/bin/env python3
"""Debug script to check for malformed columns in snapshots"""

import json
from pathlib import Path

def check_snapshot(snapshot_path):
    """Check a snapshot for columns missing required fields"""
    print(f"\n{'='*60}")
    print(f"Checking: {snapshot_path.name}")
    print('='*60)
    
    snapshot = json.loads(snapshot_path.read_text())
    state = snapshot.get('state', {})
    
    issues_found = False
    
    for catalog in state.get('catalogs', []):
        cat_name = catalog.get('name', catalog.get('id'))
        for schema in catalog.get('schemas', []):
            sch_name = schema.get('name', schema.get('id'))
            for table in schema.get('tables', []):
                tbl_name = table.get('name', table.get('id'))
                
                for i, column in enumerate(table.get('columns', [])):
                    col_id = column.get('id', f'index_{i}')
                    
                    # Check required fields
                    missing = []
                    if 'id' not in column:
                        missing.append('id')
                    if 'name' not in column:
                        missing.append('name')
                    if 'type' not in column:
                        missing.append('type')
                    
                    if missing:
                        issues_found = True
                        print(f"\n❌ ISSUE FOUND:")
                        print(f"   Location: {cat_name}.{sch_name}.{tbl_name}")
                        print(f"   Column ID: {col_id}")
                        print(f"   Missing required fields: {', '.join(missing)}")
                        print(f"   Fields present: {list(column.keys())}")
                        print(f"   Column data: {json.dumps(column, indent=2)}")
                    else:
                        # Check optional fields
                        has_comment = 'comment' in column
                        has_tags = 'tags' in column and column['tags']
                        status = []
                        if has_comment:
                            status.append('✓ comment')
                        if has_tags:
                            status.append(f'✓ tags({len(column["tags"])})')
                        
                        if not issues_found:  # Only print if no issues yet
                            print(f"   ✓ {cat_name}.{sch_name}.{tbl_name}.{column['name']}: {column['type']}")
                            if status:
                                print(f"      {', '.join(status)}")
    
    if not issues_found:
        print("\n✅ No issues found - all columns have required fields!")
    
    return issues_found

# Main
schematic_dir = Path('.schematic')

if not schematic_dir.exists():
    print("❌ .schematic directory not found!")
    print("Run this script from your project root directory.")
    exit(1)

# Check project file
project_file = schematic_dir / 'project.json'
if project_file.exists():
    project = json.loads(project_file.read_text())
    print(f"\n📋 Project: {project.get('name')}")
    print(f"   Latest snapshot: {project.get('latestSnapshot')}")
    
    # Check latest snapshot
    latest_version = project.get('latestSnapshot')
    if latest_version:
        snapshot_file = schematic_dir / 'snapshots' / f'{latest_version}.json'
        if snapshot_file.exists():
            has_issues = check_snapshot(snapshot_file)
            if has_issues:
                print("\n" + "="*60)
                print("RECOMMENDATION:")
                print("="*60)
                print("Your snapshot has malformed column data.")
                print("This could be due to:")
                print("  1. Manual editing of snapshot files")
                print("  2. Version incompatibility")
                print("  3. Corrupted data")
                print("\nSuggestions:")
                print("  - Check if columns were manually edited")
                print("  - Recreate the snapshot using the UI")
                print("  - Check git history for unexpected changes")
        else:
            print(f"\n❌ Snapshot file not found: {snapshot_file}")
else:
    print("❌ project.json not found!")

print("\n")

