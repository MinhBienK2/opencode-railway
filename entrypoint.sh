#!/bin/sh
set -e

# Nếu người dùng muốn tắt Tor để chạy 100% Direct IP
if [ "$DISABLE_TOR" = "true" ] || [ "$DISABLE_TOR" = "1" ]; then
    echo "[Gateway] Tor is disabled via DISABLE_TOR=true. Running in 100% Direct IP mode."
    exec /app/opencode-railway
fi

TOR_COUNT=${TOR_COUNT:-4}
echo "[Gateway] Initializing $TOR_COUNT Tor background daemons for Multi-Egress SOCKS5..."

TOR_INSTANCES=""
TOR_PIDS=""

cleanup() {
    echo "[Gateway] Shutting down Tor daemons..."
    for pid in $TOR_PIDS; do
        kill -TERM "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

i=1
while [ "$i" -le "$TOR_COUNT" ]; do
    socks_port=$((9050 + (i - 1) * 2))
    ctrl_port=$((socks_port + 1))
    data_dir="/tmp/tor_data_$i"

    mkdir -p "$data_dir"
    chmod 700 "$data_dir"

    tor_conf="/tmp/tor_$i.conf"
    cat <<EOF > "$tor_conf"
SocksPort 127.0.0.1:$socks_port
ControlPort 127.0.0.1:$ctrl_port
DataDirectory $data_dir
Log notice file /tmp/tor_$i.log
EOF

    tor -f "$tor_conf" &
    pid=$!
    TOR_PIDS="$TOR_PIDS $pid"

    if [ -z "$TOR_INSTANCES" ]; then
        TOR_INSTANCES="127.0.0.1:$socks_port:$ctrl_port"
    else
        TOR_INSTANCES="$TOR_INSTANCES,127.0.0.1:$socks_port:$ctrl_port"
    fi

    i=$((i + 1))
done

export TOR_INSTANCES="$TOR_INSTANCES"
echo "[Gateway] Tor cluster configured: $TOR_INSTANCES"

echo "[Gateway] Waiting for Tor SOCKS ports to become ready..."
max_wait=15
count=0
while [ "$count" -lt "$max_wait" ]; do
    if nc -z 127.0.0.1 9050 2>/dev/null; then
        echo "[Gateway] Tor node 1 is online and ready on port 9050!"
        break
    fi
    sleep 1
    count=$((count + 1))
done
echo "[Gateway] Launching Opencode Railway Gateway..."
exec /app/opencode-railway
