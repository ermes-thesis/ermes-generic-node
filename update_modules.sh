#!/usr/bin/env bash

ROOT_DIR="$(pwd)"

find "$ROOT_DIR" -name go.mod -type f | while read -r modfile; do
    module_dir="$(dirname "$modfile")"

    echo "Updating module in: $module_dir"

    (
        cd "$module_dir"
        go get -u ./...
        go mod tidy
    )

    echo "Done: $module_dir"
    echo
done

