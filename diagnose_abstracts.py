import inspect
from arcfs.core.base_handler import ArchiveHandler
from arcfs.handlers.xz_handler import XzHandler
from arcfs.handlers.zip_handler import ZipHandler
from arcfs.handlers.gzip_handler import GzipHandler
from arcfs.handlers.bzip2_handler import Bzip2Handler
from arcfs.handlers.tar_handler import TarHandler


def print_abstracts_and_impls(cls):
    print(f"\nClass: {cls.__name__}")
    # Abstract methods required by this class
    abstracts = set(getattr(cls, "__abstractmethods__", set()))
    print(f"  Abstract methods: {sorted(list(abstracts))}")
    # All methods actually implemented
    methods = set(
        name for name, obj in inspect.getmembers(cls)
        if inspect.isfunction(obj) or inspect.ismethod(obj)
    )
    print(f"  Implemented methods: {sorted(list(methods))}")
    # Show any missing
    missing = abstracts - methods
    if missing:
        print(f"  MISSING: {sorted(list(missing))}")
    else:
        print("  All abstract methods implemented.")


def main():
    print("\n--- ArchiveHandler Abstract Methods Diagnostic ---")
    for handler in [XzHandler, ZipHandler, GzipHandler, Bzip2Handler, TarHandler]:
        print_abstracts_and_impls(handler)

if __name__ == "__main__":
    main()
