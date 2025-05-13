#!/usr/bin/env python3
"""
Diagnostic script to troubleshoot import issues
"""

import os
import sys
import inspect

# Add the parent directory to sys.path if needed
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)
    print(f"Added {parent_dir} to sys.path")
else:
    print(f"{parent_dir} already in sys.path")

# Try to import the module
print("Attempting to import arcfs module...")
try:
    import arcfs
    print("Successfully imported arcfs")
    print(f"arcfs module location: {arcfs.__file__}")
    print(f"arcfs module content: {dir(arcfs)}")
    
    # Check if ArchiveFS exists directly in the module
    if hasattr(arcfs, 'ArchiveFS'):
        print("ArchiveFS exists in arcfs module")
        print(f"ArchiveFS type: {type(arcfs.ArchiveFS)}")
    else:
        print("ArchiveFS does NOT exist in arcfs module")
        
    # Check if we can import it directly
    try:
        from arcfs import ArchiveFS
        print("Successfully imported ArchiveFS from arcfs")
    except ImportError as e:
        print(f"Failed to import ArchiveFS from arcfs: {e}")
        
    # See if it's defined in __init__.py
    if os.path.exists(arcfs.__file__):
        with open(arcfs.__file__, 'r') as f:
            init_content = f.read()
        
        print("\nChecking __init__.py content:")
        if "class ArchiveFS" in init_content:
            print("ArchiveFS class is defined in __init__.py")
        else:
            print("ArchiveFS class is NOT defined in __init__.py")
            
        if "__all__" in init_content:
            print("__all__ is defined in __init__.py")
            # Extract __all__ value
            import re
            all_match = re.search(r'__all__\s*=\s*\[(.*?)\]', init_content, re.DOTALL)
            if all_match:
                print(f"__all__ value: {all_match.group(1)}")
        else:
            print("__all__ is NOT defined in __init__.py")
            
except ImportError as e:
    print(f"Failed to import arcfs: {e}")
    
# Check the Python path
print("\nPython path:")
for p in sys.path:
    print(f"  {p}")

print("\nCurrent directory structure:")
os.system(f"ls -la {parent_dir}")
print("\narchfs directory structure:")
os.system(f"ls -la {parent_dir}/arcfs")
