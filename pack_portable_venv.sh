
#!/bin/bash

# Check if a virtual environment is activated
if [ -z "$VIRTUAL_ENV" ]; then
    echo "Error: No virtual environment is activated."
    exit 1
fi

# Step 1: Set the path to the activated virtual environment
VENV_PATH="$VIRTUAL_ENV"

# Step 2: Get the Python binary path within the virtual environment
# This assumes the active Python binary is from the venv
PYTHON_BIN="$(which python)"

# Verify that the Python binary exists
if [ ! -f "$PYTHON_BIN" ]; then
    echo "Error: Python binary not found in the activated virtual environment."
    exit 1
fi

# Step 3: Copy the Python binary into the virtual environment (if not already present)
cp "$PYTHON_BIN" "$VENV_PATH/bin/"

# Step 4: Identify and copy required shared libraries
# Use `ldd` to find the shared libraries required by the Python binary
ldd "$PYTHON_BIN" | grep "=>" | awk '{print $3}' | while read -r LIBRARY_PATH; do
    # Only copy the library if it exists and is not already in the venv's lib directory
    if [ -f "$LIBRARY_PATH" ]; then
        cp "$LIBRARY_PATH" "$VENV_PATH/lib/"
    fi
done

# Step 5: Adjust the shebangs in the activate script (optional)
sed -i "s|^#!/usr/bin/env python|#!$VENV_PATH/bin/python|" "$VENV_PATH/bin/activate"

echo "Virtual environment at $VENV_PATH has been made portable."
