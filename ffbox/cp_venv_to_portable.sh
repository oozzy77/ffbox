#!/bin/bash

# Check if a virtual environment is activated
if [ -z "$VIRTUAL_ENV" ]; then
    echo "Error: No virtual environment is activated."
    exit 1
fi

# Set paths
ORIGINAL_VENV="$VIRTUAL_ENV"
PORTABLE_VENV="$(dirname "$ORIGINAL_VENV")/portable_venv"

echo "Original VENV_PATH: $ORIGINAL_VENV"
echo "Creating a copy of the virtual environment at: $PORTABLE_VENV"

# Step 1: Copy the entire virtual environment to a new directory (portable_venv)
cp -r "$ORIGINAL_VENV" "$PORTABLE_VENV"

# Step 2: Replace symbolic links with actual binaries in bin/
for PY_BIN in "$PORTABLE_VENV/bin/python" "$PORTABLE_VENV/bin/python3" "$PORTABLE_VENV/bin/python3.12"; do
    # Check if it's a symbolic link
    if [ -L "$PY_BIN" ]; then
        # Get the real path of the target
        REAL_BIN="$(readlink -f "$PY_BIN")"
        
        # Remove the symbolic link
        rm "$PY_BIN"
        
        # Copy the actual binary to the location of the symbolic link
        cp "$REAL_BIN" "$PY_BIN"
        
        echo "Replaced symbolic link $PY_BIN with actual binary from $REAL_BIN"
    fi
done

# Step 3: Find all remaining symbolic links and replace them with actual files
find "$PORTABLE_VENV" -type l | while read -r SYMLINK_PATH; do
    REAL_PATH="$(readlink -f "$SYMLINK_PATH")"
    if [ ! -f "$REAL_PATH" ]; then
        echo "Warning: Target $REAL_PATH does not exist for symlink $SYMLINK_PATH"
        continue
    fi

    rm "$SYMLINK_PATH"
    cp "$REAL_PATH" "$SYMLINK_PATH"
    echo "Replaced symbolic link $SYMLINK_PATH with actual file from $REAL_PATH"
done

# Step 4: Update paths in the activate script
# Update shebang in activate script to point to portable Python binary
# sed -i "1s|.*|#!$(dirname "$(dirname "$BASH_SOURCE")")/bin/python|" "$PORTABLE_VENV/bin/activate"

# Replace all instances of the original venv path with literal string of "$(dirname "$(dirname "$BASH_SOURCE")")" 
# after replace activate should look like: export VIRTUAL_ENV="$(dirname "$(dirname "$BASH_SOURCE")")"
sed -i 's|'"$ORIGINAL_VENV"'|$(dirname "$(dirname "$BASH_SOURCE")")|g' "$PORTABLE_VENV/bin/activate"

# Step 5: Verify symbolic links have been replaced
echo "Checking for remaining symbolic links in $PORTABLE_VENV:"
find "$PORTABLE_VENV" -type l

echo "Portable virtual environment created at $PORTABLE_VENV"
