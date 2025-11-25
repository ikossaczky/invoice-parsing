#!/bin/bash
# Reset invoice parsing setup - removes all data and recreates empty folders

# Get the project root directory (parent of scripts folder)
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Resetting invoice parsing setup..."
echo "Project root: $PROJECT_ROOT"

# Delete folders
echo "Deleting databases folder..."
rm -rf "$PROJECT_ROOT/databases"

echo "Deleting vectorstores folder..."
rm -rf "$PROJECT_ROOT/vectorstores"

echo "Deleting data_landing folder..."
rm -rf "$PROJECT_ROOT/data_landing"

# Recreate empty folders
echo "Recreating empty folders..."
mkdir -p "$PROJECT_ROOT/databases"
mkdir -p "$PROJECT_ROOT/vectorstores"
mkdir -p "$PROJECT_ROOT/data_landing"

echo "✓ Reset complete!"
echo "  - databases/ (empty)"
echo "  - vectorstores/ (empty)"
echo "  - data_landing/ (empty)"
