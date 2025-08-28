#!/bin/bash

cleanup() {
    echo "--- SHUTDOWN SIGNAL RECEIVED. KILLING CHILD PROCESSES... ---"
    pkill -P $$ # Mata a todos los hijos de este script
    echo "--- CLEANUP COMPLETE. ---"
    exit
}
trap cleanup SIGINT SIGTERM

echo "--- LAUNCHER: Starting ComfyUI in the background... ---"
cd /workspace/ComfyUI
python main.py --listen 0.0.0.0 --port 8188 &
COMFYUI_PID=$!
echo "--- LAUNCHER: ComfyUI running with PID: $COMFYUI_PID ---"

echo "--- LAUNCHER: Waiting for ComfyUI to be ready... ---"
while ! curl -s --head http://127.0.0.1:8188/history | head -n 1 | grep "200 OK" > /dev/null; do
    sleep 1
done

echo "--- LAUNCHER: ComfyUI is ready. Giving 15 seconds grace period for nodes... ---"
sleep 15

echo "--- LAUNCHER: Starting Flask handler... ---"
cd /workspace
python -u app_handler.py &

wait $COMFYUI_PID
