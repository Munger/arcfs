#!/bin/bash
# Usage: ./run_tests.sh [all|<test_module>[.<TestClass>[.<test_method>]] ...]
# Activates the venv and runs the specified unittest(s)

usage() {
    echo "Usage: $0 [--debug-level=N] [all|<test_module>[.<TestClass>[.<test_method>]] ...]"
    echo "Runs Python unittests in the ARCFS project."
    echo "  --debug-level=N       Set ARCFS_DEBUG_LEVEL to N for debug output (0-3)"
    echo "  all                  Run all tests (default)"
    echo "  <test_module>        Run all tests in a module (e.g. test_tar_handler)"
    echo "  <test_module>.<TestClass>         Run all tests in a class"
    echo "  <test_module>.<TestClass>.<test_method>  Run a specific test method"
    echo "  Multiple targets may be specified, separated by spaces."
    echo "  -h, --help           Show this help message and exit"
}

# Exit on error
set -e

# Ensure script runs from project root
cd "$(dirname "$0")"

# Activate the virtual environment
source ./venv/bin/activate

# Ensure src is on the PYTHONPATH so 'arcfs' is importable
export PYTHONPATH="$PYTHONPATH:$(pwd)/src"

# Show usage if -h or --help is given
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    usage
    exit 0
fi

# Parse optional --debug-level argument
DEBUG_LEVEL=""
test_args=()
for arg in "$@"; do
    if [[ "$arg" =~ ^--debug-level=([0-9]+)$ ]]; then
        DEBUG_LEVEL="${BASH_REMATCH[1]}"
    elif [[ "$arg" != "-h" && "$arg" != "--help" ]]; then
        test_args+=("$arg")
    fi
done

# Export debug level if set
if [[ -n "$DEBUG_LEVEL" ]]; then
    export ARCFS_DEBUG_LEVEL="$DEBUG_LEVEL"
fi

# If no arguments or 'all', run all tests
if [[ ${#test_args[@]} -eq 0 || "${test_args[0]}" == "all" ]]; then
    python -m unittest discover -v
    exit $?
fi

# Run specified tests (may be multiple)
python -m unittest -v "${test_args[@]}"
