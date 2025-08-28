import os
import subprocess
import base64
from typing import Dict, Any
import json
import uuid
import shutil
import urllib.request
import urllib.parse
import time
import sys
import threading
from flask import Flask, request, jsonify

# --- RUTAS ---
WORKSPACE_DIR = "/workspace"
COMFYUI_PATH = os.path.join(WORKSPACE_DIR, "ComfyUI")
INPUT_DIR = os.path.join(COMFYUI_PATH, "input")
OUTPUT_DIR = os.path.join(COMFYUI_PATH, "output")
TEMP_DIR = "/tmp/runpod-workspace"

def log_subprocess_output(pipe):
    for line in iter(pipe.readline, b''):
        print(f"[ComfyUI] {line.decode('utf-8').strip()}", flush=True)

def start_comfyui_server():
    COMFYUI_URL = "http://127.0.0.1:8188"
    print(f"Starting ComfyUI server using 'Nuke From Orbit' method...", flush=True)
    
    # Este es el comando que escribiríamos a mano en la terminal.
    # Lo envolvemos en una llamada a 'bash -c'. Esto crea un nuevo shell,
    # cambia de directorio, y LUEGO ejecuta python. Es la forma más robusta
    # de aislar el subproceso de cualquier influencia extraña del script padre.
    full_command = f"cd {COMFYUI_PATH} && python main.py --dont-print-server --listen 0.0.0.0 --port 8188"

    server_process = subprocess.Popen(
        ['/bin/bash', '-c', full_command], # Ejecutamos bash, no python directamente
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=os.environ # Mantenemos esto por si acaso
    )

    stdout_thread = threading.Thread(target=log_subprocess_output, args=(server_process.stdout,))
    stderr_thread = threading.Thread(target=log_subprocess_output, args=(server_process.stderr,))
    stdout_thread.daemon = True
    stderr_thread.daemon = True
    stdout_thread.start()
    stderr_thread.start()

    print("ComfyUI process started. Waiting for server to become responsive...", flush=True)
    timeout = 300
    start_time = time.time()
    
    # Damos un tiempo de arranque inicial generoso antes de empezar a comprobar.
    time.sleep(20)

    while time.time() - start_time < timeout:
        if server_process.poll() is not None:
            raise RuntimeError(f"ComfyUI process terminated unexpectedly with code {server_process.poll()}. Check logs for errors.")
        try:
            with urllib.request.urlopen(f"{COMFYUI_URL}/history", timeout=5) as response:
                if response.status == 200:
                    print("ComfyUI server is responsive and ready.", flush=True)
                    print("Giving ComfyUI an extra 10 seconds to finish loading custom nodes...", flush=True)
                    time.sleep(10)
                    return server_process
        except Exception as e:
            print(f"Server not ready yet, waiting... (Error: {e})", flush=True)
            time.sleep(5)

    raise TimeoutError("Timeout: ComfyUI server did not become responsive.")

# --- INICIALIZACIÓN GLOBAL ---
try:
    server_process = start_comfyui_server()
    CLIENT_ID = str(uuid.uuid4())
    COMFYUI_API_URL = "http://127.0.0.1:8188"
except (RuntimeError, TimeoutError) as e:
    print(f"CRITICAL ERROR: Could not start ComfyUI. Exiting.", file=sys.stderr, flush=True)
    print(e, file=sys.stderr, flush=True)
    sys.exit(1)

# --- LÓGICA DE PROCESAMIENTO ---
def queue_prompt(prompt: Dict[str, Any], client_id: str) -> Dict[str, Any]:
    p = {"prompt": prompt, "client_id": client_id}
    data = json.dumps(p).encode('utf-8')
    req = urllib.request.Request(f"{COMFYUI_API_URL}/prompt", data=data)
    with urllib.request.urlopen(req) as response:
        return json.loads(response.read())

def get_history(prompt_id: str) -> Dict[str, Any]:
    try:
        with urllib.request.urlopen(f"{COMFYUI_API_URL}/history/{prompt_id}") as response:
            return json.loads(response.read())
    except urllib.error.URLError: return {}

def clean_directory(directory: str):
    if os.path.exists(directory): shutil.rmtree(directory)
    os.makedirs(directory, exist_ok=True)

def process_video_with_comfyui(frames_input_dir: str, face_ref_path: str, prompt_text: str) -> str:
    workflow_path = os.path.join(WORKSPACE_DIR, "workflow_api.json")
    with open(workflow_path, 'r') as f:
        prompt_workflow = json.load(f)

    # Inyectamos los datos dinámicos en el workflow
    prompt_workflow["4"]["inputs"]["text"] = prompt_text
    prompt_workflow["6"]["inputs"]["image"] = os.path.basename(face_ref_path)
    prompt_workflow["7"]["inputs"]["directory"] = frames_input_dir

    response = queue_prompt(prompt_workflow, CLIENT_ID)
    if 'prompt_id' not in response:
        raise RuntimeError(f"Failed to queue prompt. API response: {response}")
    return response['prompt_id']

def handler(job: Dict[str, Any]) -> Dict[str, Any]:
    job_input = job.get('input', {})
    video_b64 = job_input.get('video_b64')
    face_b64 = job_input.get('face_b64')

    if not video_b64 or not face_b64:
        return {"error": "Missing 'video_b64' or 'face_b64' in job input."}

    clean_directory(TEMP_DIR)
    clean_directory(INPUT_DIR)
    clean_directory(OUTPUT_DIR)

    video_path_on_disk = os.path.join(TEMP_DIR, "input_video.mp4")
    face_path_on_disk = os.path.join(TEMP_DIR, "input_face.png")

    try:
        print("Decoding Base64 input files...", flush=True)
        with open(video_path_on_disk, "wb") as f: f.write(base64.b64decode(video_b64))
        with open(face_path_on_disk, "wb") as f: f.write(base64.b64decode(face_b64))
        face_image_path_in_comfyui = os.path.join(INPUT_DIR, "face_reference.png")
        shutil.copy(face_path_on_disk, face_image_path_in_comfyui)
        print("Input files successfully decoded and prepared.", flush=True)
    except Exception as e:
        return {"error": f"Error decoding or preparing input files: {str(e)}"}

    audio_path = os.path.join(TEMP_DIR, "original_audio.aac")
    frames_input_dir = os.path.join(INPUT_DIR, "video_frames")
    os.makedirs(frames_input_dir, exist_ok=True)
    try:
        print("Starting ffmpeg pre-production...", flush=True)
        subprocess.run(f"ffmpeg -i {video_path_on_disk} -vn -acodec copy {audio_path}", shell=True, check=True, capture_output=True, text=True)
        subprocess.run(f"ffmpeg -i {video_path_on_disk} {os.path.join(frames_input_dir, 'frame_%04d.png')}", shell=True, check=True, capture_output=True, text=True)
        print("ffmpeg pre-production successful.", flush=True)
    except subprocess.CalledProcessError as e:
        return {"error": f"FFmpeg pre-production failed. Stderr: {e.stderr}"}

    try:
        print("Executing ComfyUI workflow...", flush=True)
        prompt_id = process_video_with_comfyui(frames_input_dir, face_image_path_in_comfyui, job_input.get('prompt', ''))
        timeout = 3600
        start_time = time.time()
        while time.time() - start_time < timeout:
            history = get_history(prompt_id)
            if prompt_id in history and history[prompt_id].get('outputs'):
                print("ComfyUI workflow completed.", flush=True); break
            print("Waiting for ComfyUI workflow to complete...", flush=True)
            time.sleep(10)
        else:
             return {"error": "Timeout: ComfyUI did not complete the workflow in time."}
    except Exception as e:
        return {"error": f"ComfyUI workflow execution error: {str(e)}"}

    final_video_path = os.path.join(TEMP_DIR, "final_video.mp4")
    video_no_audio_path = os.path.join(TEMP_DIR, 'video_no_audio.mp4')
    try:
        print("Starting ffmpeg post-production...", flush=True)
        ffmpeg_input_pattern = os.path.join(OUTPUT_DIR, 'ResultFrames_%05d.png')
        framerate = 24
        cmd1 = f"ffmpeg -framerate {framerate} -i \"{ffmpeg_input_pattern}\" -c:v libx264 -pix_fmt yuv420p {video_no_audio_path}"
        subprocess.run(cmd1, shell=True, check=True, capture_output=True, text=True)
        cmd2 = f"ffmpeg -i {video_no_audio_path} -i {audio_path} -c:v copy -c:a aac -shortest {final_video_path}"
        subprocess.run(cmd2, shell=True, check=True, capture_output=True, text=True)
        print("ffmpeg post-production successful.", flush=True)
    except subprocess.CalledProcessError as e:
        return {"error": f"FFmpeg post-production failed. Stderr: {e.stderr}"}

    try:
        if not os.path.exists(final_video_path):
            return {"error": "Final video file was not created by FFmpeg."}
        with open(final_video_path, "rb") as f:
            video_b64 = base64.b64encode(f.read()).decode('utf-8')
        return {"video_b64": video_b64}
    except Exception as e:
        return {"error": f"Error encoding the result: {str(e)}"}
    finally:
        clean_directory(TEMP_DIR)
        clean_directory(INPUT_DIR)
        clean_directory(OUTPUT_DIR)

# --- SERVIDOR WEB ---
app = Flask(__name__)
@app.route('/run', methods=['POST'])
def run_sync():
    print("Received a request on /run", flush=True)
    job_payload = request.get_json()
    result = handler(job_payload)
    print("Handler processing finished. Sending response.", flush=True)
    return jsonify(result)

if __name__ == "__main__":
    print("Initialization complete. Starting Flask server...", flush=True)
    app.run(host='0.0.0.0', port=8888)
