#!/bin/bash
# setup-secrets.sh - Interactive Docker secret setup for monitor app
#
# Creates/updates:
# - nodes_config (single Docker secret containing all remote node properties)

set -euo pipefail

TTY=/dev/tty
NODES_SECRET_NAME="nodes_config"
HARD_CODED_SSH_PORT=22
MAX_REMOTE_NODES=20

if [ ! -r "$TTY" ]; then
    echo "ERROR: No interactive terminal available ($TTY not readable)."
    echo "Run this script from an interactive shell."
    exit 1
fi

secret_exists() {
    local secret_name="$1"
    docker secret ls --format '{{.Name}}' | grep -Fqx "$secret_name"
}

is_yes() {
    local v
    v="${1:-}"
    v="${v,,}"
    [ "$v" = "y" ] || [ "$v" = "yes" ]
}

SWARM_STATE=$(docker info --format '{{.Swarm.LocalNodeState}}' 2>/dev/null || true)
IS_MANAGER=$(docker info --format '{{.Swarm.ControlAvailable}}' 2>/dev/null || true)

if [ "$SWARM_STATE" != "active" ]; then
    echo "ERROR: Docker Swarm is not active for this Docker daemon."
    echo "Run: docker swarm init"
    exit 1
fi

if [ "$IS_MANAGER" != "true" ]; then
    echo "ERROR: This node is not a Swarm manager; it cannot create secrets."
    echo "Run this script on a manager node (or set DOCKER_HOST to a manager)."
    exit 1
fi

tmp_rows_file=$(mktemp)
trap 'rm -f "$tmp_rows_file"' EXIT

while true; do
    read -r -p "How many remote nodes do you want to monitor? (max ${MAX_REMOTE_NODES}) " NODE_COUNT <"$TTY"
    NODE_COUNT="${NODE_COUNT//[$'\t\r\n']/}"
    if [[ "$NODE_COUNT" =~ ^[0-9]+$ ]] && [ "$NODE_COUNT" -le "$MAX_REMOTE_NODES" ]; then
        break
    fi
    echo "Please enter a valid number between 0 and ${MAX_REMOTE_NODES}."
done

if [ "$NODE_COUNT" -eq 0 ]; then
    echo "No remote nodes selected. Creating nodes_config with an empty list."
    echo ""
fi

for (( i=1; i<=NODE_COUNT; i++ )); do
    echo "Remote node ${i}/${NODE_COUNT}"
    while true; do
        read -r -p "Node name: " NODE_NAME <"$TTY"
        NODE_NAME="${NODE_NAME//[$'\t\r\n']/}"
        if [ -z "$NODE_NAME" ]; then
            echo "Node name cannot be empty."
            continue
        fi
        break
    done

    while true; do
        read -r -p "IP: " NODE_HOST <"$TTY"
        NODE_HOST="${NODE_HOST//[$'\t\r\n']/}"
        if [ -z "$NODE_HOST" ]; then
            echo "IP cannot be empty."
            continue
        fi
        break
    done

    while true; do
        read -r -p "Username: " NODE_USER <"$TTY"
        NODE_USER="${NODE_USER//[$'\t\r\n']/}"
        if [ -z "$NODE_USER" ]; then
            echo "Username cannot be empty."
            continue
        fi
        break
    done

    while true; do
        read -r -s -p "Password: " NODE_PASSWORD <"$TTY"
        echo ""
        read -r -s -p "Confirm password: " NODE_PASSWORD_CONFIRM <"$TTY"
        echo ""

        if [ -z "$NODE_PASSWORD" ]; then
            echo "Password cannot be empty."
            continue
        fi

        if [ "$NODE_PASSWORD" != "$NODE_PASSWORD_CONFIRM" ]; then
            echo "Passwords do not match. Try again."
            continue
        fi
        break
    done

    printf '%s\t%s\t%s\t%s\n' "$NODE_NAME" "$NODE_HOST" "$NODE_USER" "$NODE_PASSWORD" >> "$tmp_rows_file"
    echo "Added node: ${NODE_NAME}"
    echo ""
done

nodes_json=$(python3 - "$tmp_rows_file" "$HARD_CODED_SSH_PORT" <<'PY'
import json
import sys

rows_path = sys.argv[1]
port = int(sys.argv[2])
nodes = []
seen = set()

with open(rows_path, 'r', encoding='utf-8') as f:
    for line in f:
        line = line.rstrip('\n')
        if not line:
            continue
        parts = line.split('\t')
        if len(parts) != 4:
            continue
        name, host, username, password = [p.strip() for p in parts]
        if not name or not host or not username or not password:
            continue
        if name in seen:
            print(f"ERROR: Duplicate node name: {name}", file=sys.stderr)
            sys.exit(2)
        seen.add(name)
        nodes.append({
            "name": name,
            "host": host,
            "username": username,
            "password": password,
            "type": "ssh",
            "port": port
        })

print(json.dumps(nodes, indent=2))
PY
)

if [ $? -ne 0 ]; then
    echo "ERROR: Failed building nodes JSON payload."
    exit 1
fi

if secret_exists "$NODES_SECRET_NAME"; then
    read -r -p "Secret ${NODES_SECRET_NAME} already exists. Replace it? (yes/no): " REPLACE_SECRET <"$TTY"
    if ! is_yes "$REPLACE_SECRET"; then
        echo "Skipped updating ${NODES_SECRET_NAME}."
        exit 0
    fi
    docker secret rm "$NODES_SECRET_NAME"
fi

SECRET_ID=$(printf '%s' "$nodes_json" | docker secret create "$NODES_SECRET_NAME" -)

echo ""
echo "Excellent!"
echo "This is the new ${NODES_SECRET_NAME} Docker Secret ID: ${SECRET_ID}"
echo ""
echo "Next step: deploy the monitor app, wait a minute, then start using the app."
