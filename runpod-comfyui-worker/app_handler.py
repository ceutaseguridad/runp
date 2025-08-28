import runpod
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

COMFYUI_URL = "http://127.0.0.1:8188"
COMFYUI_PATH = "/ComfyUI"
INPUT_DIR = os.path.join(COMFYUI_PATH, "input")
OUTPUT_DIR = os.path.join(COMFYUI_PATH, "output")
TEMP_DIR = "/tmp/runpod-workspace"

def log_subprocess_output(pipe):
    for line in iter(pipe.readline, b''):
        print(f"[ComfyUI] {line.decode('utf-8').strip()}", flush=True)

def start_comfyui_server():
    os.chdir(COMFYUI_PATH)
    command = "python main.py --dont-print-server --listen 0.0.0.0 --port 8188"
    
    server_process = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    stdout_thread = threading.Thread(target=log_subprocess_output, args=(server_process.stdout,))
    stderr_thread = threading.Thread(target=log_subprocess_output, args=(server_process.stderr,))
    stdout_thread.daemon = True
    stderr_thread.daemon = True
    stdout_thread.start()
    stderr_thread.start()

    timeout = 180
    start_time = time.time()
    while time.time() - start_time < timeout:
        if server_process.poll() is not None:
            raise RuntimeError(f"ComfyUI process terminated unexpectedly with code {server_process.poll()}.")

        try:
            with urllib.request.urlopen(f"{COMFYUI_URL}/history", timeout=2) as response:
                if response.status == 200:
                    print("ComfyUI server started successfully.")
                    return server_process
        except Exception:
            time.sleep(2)
            
    raise TimeoutError("Timeout: ComfyUI server did not respond within 3 minutes.")

try:
    server_process = start_comfyui_server()
    CLIENT_ID = str(uuid.uuid4())
except (RuntimeError, TimeoutError) as e:
    print(f"Critical error during initialization: {e}", file=sys.stderr)
    sys.exit(1)

def queue_prompt(prompt: Dict[str, Any], client_id: str) -> Dict[str, Any]:
    p = {"prompt": prompt, "client_id": client_id}
    data = json.dumps(p).encode('utf-8')
    req = urllib.request.Request(f"{COMFYUI_URL}/prompt", data=data)
    with urllib.request.urlopen(req) as response:
        return json.loads(response.read())

def get_history(prompt_id: str) -> Dict[str, Any]:
    try:
        with urllib.request.urlopen(f"{COMFYUI_URL}/history/{prompt_id}") as response:
            return json.loads(response.read())
    except urllib.error.URLError:
        return {}

def clean_directory(directory: str):
    if os.path.exists(directory):
        shutil.rmtree(directory)
    os.makedirs(directory, exist_ok=True)

def process_video_with_comfyui(frames_input_dir: str, face_ref_path: str, prompt_text: str, workflow_json_path: str) -> str:
    with open(workflow_json_path, 'r') as f:
        prompt_workflow = json.load(f)

    prompt_workflow["4"]["inputs"]["text"] = prompt_text
    prompt_workflow["6"]["inputs"]["image"] = os.path.basename(face_ref_path)
    prompt_workflow["7"]["inputs"]["directory"] = frames_input_dir

    response = queue_prompt(prompt_workflow, CLIENT_ID)
    if 'prompt_id' not in response:
        raise RuntimeError(f"Failed to queue prompt. API response: {response}")
    return response['prompt_id']

def handler(job: Dict[str, Any]) -> Dict[str, Any]:
    job_input = job.get('input', {})
    
    expected_keys = ['video_filename', 'face_filename', 'prompt']
    if not all(k in job_input for k in expected_keys):
        return {"error": f"Missing required inputs. Expected: {expected_keys}, got: {list(job_input.keys())}"}

    clean_directory(TEMP_DIR)
    clean_directory(INPUT_DIR)
    clean_directory(OUTPUT_DIR)
    
    try:
        video_filename = job_input['video_filename']
        face_filename = job_input['face_filename']
        
        video_path_on_disk = os.path.join("/", video_filename)
        face_path_on_disk = os.path.join("/", face_filename)
        
        if not os.path.exists(video_path_on_disk) or not os.path.exists(face_path_on_disk):
            return {"error": f"Input files not found. Searched for '{video_path_on_disk}' and '{face_path_on_disk}'. Root directory contents: {os.listdir('/')}"}

        face_image_path_in_comfyui = os.path.join(INPUT_DIR, "face_reference.png")
        shutil.copy(face_path_on_disk, face_image_path_in_comfyui)
    except Exception as e:
        return {"error": f"Error preparing input files: {str(e)}"}

    audio_path = os.path.join(TEMP_DIR, "original_audio.aac")
    frames_input_dir = os.path.join(INPUT_DIR, "video_frames")
    os.makedirs(frames_input_dir, exist_ok=True)
    try:
        subprocess.run(f"ffmpeg -i {video_path_on_disk} -vn -acodec copy {audio_path}", shell=True, check=True, capture_output=True, text=True)
        subprocess.run(f"ffmpeg -i {video_path_on_disk} {os.path.join(frames_input_dir, 'frame_%04d.png')}", shell=True, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        return {"error": f"FFmpeg pre-production failed: {e.stderr}"}

    try:
        prompt_id = process_video_with_comfyui(frames_input_dir, face_image_path_in_comfyui, job_input['prompt'], "/workflow_api.json")
        
        timeout = 3600
        start_time = time.time()
        workflow_complete = False
        while time.time() - start_time < timeout:
            history = get_history(prompt_id)
            if prompt_id in history and history[prompt_id].get('outputs'):
                workflow_complete = True
                break
            time.sleep(5)
        
        if not workflow_complete:
             return {"error": "Timeout: ComfyUI did not complete the workflow in time."}
    except Exception as e:
        return {"error": f"ComfyUI workflow execution error: {str(e)}"}

    final_video_path = os.path.join(TEMP_DIR, "final_video.mp4")
    video_no_audio_path = os.path.join(TEMP_DIR, 'video_no_audio.mp4')
    try:
        ffmpeg_input_pattern = os.path.join(OUTPUT_DIR, 'ResultFrames_%05d.png')
        framerate = 24 # Assuming 24 fps, adjust if necessary
        
        cmd1 = f"ffmpeg -framerate {framerate} -i \"{ffmpeg_input_pattern}\" -c:v libx264 -pix_fmt yuv420p {video_no_audio_path}"
        subprocess.run(cmd1, shell=True, check=True, capture_output=True, text=True)
        
        cmd2 = f"ffmpeg -i {video_no_audio_path} -i {audio_path} -c:v copy -c:a aac -shortest {final_video_path}"
        subprocess.run(cmd2, shell=True, check=True, capture_output=True, text=True)
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

if __name__ == "__main__":
    if not os.path.exists("/workflow_api.json"):
        shutil.copyfile(os.path.join(COMFYUI_PATH, "../workflow_api.json"), "/workflow_api.json")
    print("Starting RunPod Serverless Worker...")
    runpod.serverless.start({"handler": handler})
