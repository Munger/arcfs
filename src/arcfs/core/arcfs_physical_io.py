"""
arcfs_physical_io.py

*** INTERNAL MODULE: DO NOT IMPORT OUTSIDE IO SUBSYSTEM ***

This file is strictly INTERNAL to ARCFS. It provides the physical and buffer-aware IO backend.

!!! MAINTAINERS: DO NOT IMPORT THIS MODULE FROM HANDLER CODE, ARCHIVE HANDLERS, OR ANY CODE OUTSIDE THE IO SUBSYSTEM !!!

- HANDLERS must use ONLY the following *public* ARCFS modules for all file and directory operations:
    * arcfs.file_operations (FileOperations)
    * arcfs.directory_operations (DirectoryOperations)
    * Other public, documented operations modules as specified in ARCFS documentation
- Handlers and top-level modules MUST NEVER import or call ArcfsPhysicalIO directly, nor any other internal IO or buffering modules.
- Violating this rule is an architectural error and may break ARCFS or compromise maintainability.
- If you need new IO functionality, add it to FileOperations, DirectoryOperations, or another public API and route it through here.

Author: Tim Hosking
Contact: https://github.com/Munger
License: MIT
"""

import os
import shutil
from typing import Optional, Union, List, BinaryIO, TextIO
from arcfs.core.logging import debug_print
from .buffering import HybridBufferedFile  # Assumed buffer class

class ArcfsPhysicalIO:
    """
    INTERNAL IO BACKEND FOR ARCFS ONLY.

    DO NOT import or use ArcfsPhysicalIO from handler modules, archive handlers, or any code outside the IO subsystem.
    All access must go through FileOperations or other public APIs.
    
    This class provides a centralized, buffer-aware interface for all physical file and directory operations.
    """
    
    @staticmethod
    def open(path, mode='r', buffering=-1, encoding=None, errors=None, newline=None):
        try:
            debug_print(f"[ArcfsPhysicalIO.open] path={path}, mode={mode}", level=2)
            # Buffer-aware: use HybridBufferedFile if managed
            if HybridBufferedFile.is_managed(path):
                debug_print(f"[ArcfsPhysicalIO.open] Using HybridBufferedFile for {path}", level=2)
                return HybridBufferedFile.open(path, mode, buffering, encoding, errors, newline)
            else:
                if path is None:
                    debug_print("[ArcfsPhysicalIO.open] [ArcfsPhysicalIO.open] ERROR: path=None passed to built-in open", level=1)
                    raise ValueError("ArcfsPhysicalIO.open: path cannot be None for built-in open()")
                debug_print(f"[ArcfsPhysicalIO.open] Using built-in open for {path}", level=2)
                return open(path, mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline)
        except Exception as e:
            debug_print(f"Exception in ArcfsPhysicalIO.open: {e}", level=1, exc=e)
            raise IOError(f"Error opening file: {e}")

    @staticmethod
    def remove(path):
        try:
            debug_print(f"[ArcfsPhysicalIO.remove] path={path}", level=2)
            if HybridBufferedFile.is_managed(path):
                debug_print(f"[ArcfsPhysicalIO.remove] Removing from HybridBufferedFile: {path}", level=2)
                HybridBufferedFile.remove(path)
            else:
                debug_print(f"[ArcfsPhysicalIO.remove] Removing from real FS: {path}", level=2)
                os.remove(path)
        except Exception as e:
            debug_print(f"Exception in ArcfsPhysicalIO.remove: {e}", level=1, exc=e)
            raise IOError(f"Error removing file: {e}")
        debug_print(f"[ArcfsPhysicalIO.remove] path={path}", level=2)
        if HybridBufferedFile.is_managed(path):
            debug_print(f"[ArcfsPhysicalIO.remove] Removing from HybridBufferedFile: {path}", level=2)
            HybridBufferedFile.remove(path)
        else:
            debug_print(f"[ArcfsPhysicalIO.remove] Removing from real FS: {path}", level=2)
            os.remove(path)

    @staticmethod
    def rename(src, dst):
        debug_print(f"[ArcfsPhysicalIO.rename] src={src}, dst={dst}", level=2)
        if HybridBufferedFile.is_managed(src):
            debug_print(f"[ArcfsPhysicalIO.rename] Renaming in HybridBufferedFile: {src} -> {dst}", level=2)
            HybridBufferedFile.rename(src, dst)
        else:
            debug_print(f"[ArcfsPhysicalIO.rename] Renaming in real FS: {src} -> {dst}", level=2)
            os.rename(src, dst)

    @staticmethod
    def mkdir(path, parents=False, exist_ok=True):
        debug_print(f"[ArcfsPhysicalIO.mkdir] path={path}, parents={parents}, exist_ok={exist_ok}", level=2)
        if HybridBufferedFile.is_managed(path):
            debug_print(f"[ArcfsPhysicalIO.mkdir] Creating dir in HybridBufferedFile: {path}", level=2)
            HybridBufferedFile.mkdir(path, parents, exist_ok)
        else:
            debug_print(f"[ArcfsPhysicalIO.mkdir] Creating dir in real FS: {path}", level=2)
            if parents:
                os.makedirs(path, exist_ok=exist_ok)
            else:
                os.mkdir(path)

    @staticmethod
    def rmdir(path, recursive=False):
        debug_print(f"[ArcfsPhysicalIO.rmdir] path={path}, recursive={recursive}", level=2)
        if HybridBufferedFile.is_managed(path):
            debug_print(f"[ArcfsPhysicalIO.rmdir] Removing dir in HybridBufferedFile: {path}", level=2)
            HybridBufferedFile.rmdir(path, recursive)
        else:
            debug_print(f"[ArcfsPhysicalIO.rmdir] Removing dir in real FS: {path}", level=2)
            if recursive:
                shutil.rmtree(path)
            else:
                os.rmdir(path)

    @staticmethod
    def stat(path):
        debug_print(f"[ArcfsPhysicalIO.stat] path={path}", level=2)
        if HybridBufferedFile.is_managed(path):
            debug_print(f"[ArcfsPhysicalIO.stat] Stat in HybridBufferedFile: {path}", level=2)
            return HybridBufferedFile.stat(path)
        else:
            debug_print(f"[ArcfsPhysicalIO.stat] Stat in real FS: {path}", level=2)
            return os.stat(path)

    @staticmethod
    def exists(path):
        debug_print(f"[ArcfsPhysicalIO.exists] path={path}", level=2)
        if HybridBufferedFile.is_managed(path):
            debug_print(f"[ArcfsPhysicalIO.exists] Exists in HybridBufferedFile: {path}", level=2)
            return HybridBufferedFile.exists(path)
        else:
            debug_print(f"[ArcfsPhysicalIO.exists] Exists in real FS: {path}", level=2)
            return os.path.exists(path)

    @staticmethod
    def listdir(path):
        debug_print(f"[ArcfsPhysicalIO.listdir] path={path}", level=2)
        if HybridBufferedFile.is_managed(path):
            debug_print(f"[ArcfsPhysicalIO.listdir] Listdir in HybridBufferedFile: {path}", level=2)
            return HybridBufferedFile.listdir(path)
        else:
            debug_print(f"[ArcfsPhysicalIO.listdir] Listdir in real FS: {path}", level=2)
            return os.listdir(path)

    # Add more methods as needed (copy, move, utime, etc.)
