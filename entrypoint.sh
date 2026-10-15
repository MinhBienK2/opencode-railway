#!/bin/sh
# Gateway tự chạy và giám sát Tor (bật theo nhu cầu khi direct bị 429), nên entrypoint chỉ cần exec.
exec /app/opencode-railway
