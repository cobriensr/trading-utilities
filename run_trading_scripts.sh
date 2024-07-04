#!/bin/bash

# Source the .env file from the same directory
set -a
source "$(dirname "$0")/.env"
set +a

# echo the FOLDER_NAME variable
echo "The folder path is: $SCRIPT_DIR"

# Use the folder in commands
ls "$SCRIPT_DIR"

# Path to your preferred Python interpreter
PYTHON="/usr/local/bin/python3.12"

# List of scripts to run
SCRIPTS=(
    "trading_pipeline_micro.py"
    "trading_pipeline_mini.py"
    "trading_removal.py"
)

# Function to run a Python script and check for errors
run_script() {
    local script="$1"
    local full_path="$SCRIPT_DIR/$script"
    if [ ! -f "$full_path" ]; then
        echo "Error: File $full_path not found"
        exit 1
    fi
    if $PYTHON "$full_path"; then
        echo "Successfully ran $script"
    else
        echo "Error running $script"
        exit 1
    fi
}

# Change to the directory containing the Python scripts
cd "$SCRIPT_DIR" || {
    echo "Error: Unable to change to directory $SCRIPT_DIR"
    exit 1
}

# Run each script
for script in "${SCRIPTS[@]}"; do
    run_script "$script"
done

# Notify the user that all scripts have completed
echo "All scripts completed successfully"