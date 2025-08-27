import runpod
import os
import subprocess
import base64
from typing import Dict
import json
import uuid
import shutil
import websocket
import urllib.request
import urllib.parse
import time
import requests

# --- Constantes y Configuración ---
COMFYUI_URL = "http://127.0.0.1:8188"
COMFYUI_PATH = "/ComfyUI"
INPUT_DIR = os.path.join(COMFYUI_PATH, "input")
OUTPUT_DIR = os.path.join(COMFYUI_PATH, "output")
TEMP_DIR = "/tmp/runpod-workspace"

# --- Inicialización del Servidor ComfyUI ---
def start_comfyui_server():
    os.chdir(COMFYUI_PATH)
    command = "python main.py --dont-print-server --listen 0.0.0.0 --port 8188"
    server_process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    # Esperar a que el servidor esté listo
    while True:
        try:
            with urllib.request.urlopen(f"{COMFYUI_URL}/history") as response:
                if response.status == 200:
                    print("Servidor ComfyUI iniciado y listo.")
                    break
        except Exception:
            print("Esperando al servidor de ComfyUI...")
            time.sleep(1)
    return server_process

server_process = start_comfyui_server()
CLIENT_ID = str(uuid.uuid4())

# --- Funciones de Comunicación con ComfyUI ---
def queue_prompt(prompt: Dict, client_id: str) -> Dict:
    p = {"prompt": prompt, "client_id": client_id}
    data = json.dumps(p).encode('utf-8')
    req = urllib.request.Request(f"{COMFYUI_URL}/prompt", data=data)
    return json.loads(urllib.request.urlopen(req).read())

def get_history(prompt_id: str) -> Dict:
    with urllib.request.urlopen(f"{COMFYUI_URL}/history/{prompt_id}") as response:
        return json.loads(response.read())

def get_image(filename, subfolder, folder_type):
    data = {"filename": filename, "subfolder": subfolder, "type": folder_type}
    url_values = urllib.parse.urlencode(data)
    with urllib.request.urlopen(f"{COMFYUI_URL}/view?{url_values}") as response:
        return response.read()

def clean_directory(directory: str):
    if os.path.exists(directory):
        shutil.rmtree(directory)
    os.makedirs(directory, exist_ok=True)

# --- Funciones de Procesamiento de Vídeo ---
def process_video_with_comfyui(frames_input_dir: str, face_ref_path: str, prompt_text: str, workflow_json_path: str):
    ws = websocket.WebSocket()
    ws.connect(f"ws://{COMFYUI_URL}/ws?clientId={CLIENT_ID}")
    
    with open(workflow_json_path, 'r') as f:
        prompt_workflow = json.load(f)

    prompt_workflow["4"]["inputs"]["text"] = prompt_text
    prompt_workflow["6"]["inputs"]["image"] = os.path.basename(face_ref_path)
    prompt_workflow["7"]["inputs"]["directory"] = os.path.basename(frames_input_dir)

    queue_prompt(prompt_workflow, CLIENT_ID)
    ws.close()

# --- El Handler Principal de RunPod ---
def handler(job: Dict) -> Dict:
    job_input = job['input']
    
    # MODIFICACIÓN CLAVE: Buscamos las claves "_s3" que nos envía la nueva app.py
    if not all(k in job_input for k in ['base_video_s3', 'face_image_s3', 'prompt']):
        return {"error": "Faltan entradas requeridas: 'base_video_s3', 'face_image_s3', 'prompt'"}

    clean_directory(TEMP_DIR)
    clean_directory(INPUT_DIR)
    clean_directory(OUTPUT_DIR)
    
    # 1. Descargar archivos de entrada desde las URLs S3
    print("Descargando archivos de entrada desde S3...")
    try:
        video_path = os.path.join(TEMP_DIR, "base_video.mp4")
        # MODIFICACIÓN CLAVE: Usamos la clave "base_video_s3"
        response_video = requests.get(job_input['base_video_s3'], stream=True)
        response_video.raise_for_status()
        with open(video_path, "wb") as f:
            for chunk in response_video.iter_content(chunk_size=8192):
                f.write(chunk)
        
        face_image_path = os.path.join(INPUT_DIR, "face_reference.png")
        # MODIFICACIÓN CLAVE: Usamos la clave "face_image_s3"
        response_face = requests.get(job_input['face_image_s3'], stream=True)
        response_face.raise_for_status()
        with open(face_image_path, "wb") as f:
            for chunk in response_face.iter_content(chunk_size=8192):
                f.write(chunk)

    except Exception as e:
        return {"error": f"Error descargando archivos desde S3: {e}"}

    # 2. Pre-Producción
    print("Iniciando pre-producción (extrayendo frames y audio)...")
    audio_path = os.path.join(TEMP_DIR, "original_audio.aac")
    frames_input_dir = os.path.join(INPUT_DIR, "video_frames")
    os.makedirs(frames_input_dir, exist_ok=True)
    try:
        subprocess.run(f"ffmpeg -i {video_path} -vn -acodec copy {audio_path}", shell=True, check=True, capture_output=True, text=True)
        subprocess.run(f"ffmpeg -i {video_path} {os.path.join(frames_input_dir, 'frame_%04d.png')}", shell=True, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        return {"error": f"FFmpeg pre-producción falló: {e.stderr}"}

    # 3. Ejecución del Workflow
    print("Ejecutando workflow de ComfyUI...")
    try:
        process_video_with_comfyui(frames_input_dir, face_image_path, job_input['prompt'], "/workflow_api.json")
    
        print("Esperando a que ComfyUI genere los frames de salida...")
        output_frames_path = OUTPUT_DIR
        timeout = 3600 # 1 hora de timeout
        start_time = time.time()
        files_found = False
        while time.time() - start_time < timeout:
            if any(f.endswith('.png') for f in os.listdir(output_frames_path)):
                print("Frames de salida detectados.")
                files_found = True
                break
            time.sleep(5)
        
        if not files_found:
             return {"error": "Timeout: ComfyUI no generó los frames de salida a tiempo."}
    except Exception as e:
        return {"error": f"Error en el workflow de ComfyUI: {e}"}

    # 4. Post-Producción
    print("Iniciando post-producción (ensamblando video final)...")
    final_video_path = os.path.join(TEMP_DIR, "final_video.mp4")
    try:
        subprocess.run(f"ffmpeg -framerate 24 -i {os.path.join(output_frames_path, 'ResultFrames_%05d.png')} -c:v libx264 -pix_fmt yuv420p {os.path.join(TEMP_DIR, 'video_no_audio.mp4')}", shell=True, check=True, capture_output=True, text=True)
        subprocess.run(f"ffmpeg -i {os.path.join(TEMP_DIR, 'video_no_audio.mp4')} -i {audio_path} -c:v copy -c:a aac -shortest {final_video_path}", shell=True, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        return {"error": f"FFmpeg post-producción falló: {e.stderr}"}

    # 5. Devolver resultado
    print("Codificando y devolviendo el resultado...")
    try:
        with open(final_video_path, "rb") as f:
            video_b64 = base64.b64encode(f.read()).decode('utf-8')
        
        shutil.rmtree(TEMP_DIR)
        return {"video_b64": video_b64}
    except Exception as e:
        return {"error": f"Error codificando el resultado: {e}"}


# --- Iniciar el servidor de RunPod ---
if __name__ == "__main__":
    if not os.path.exists("/workflow_api.json"):
        shutil.copyfile(os.path.join(COMFYUI_PATH, "../workflow_api.json"), "/workflow_api.json")
    print("Iniciando worker de ComfyUI para RunPod...")
    runpod.serverless.start({"handler": handler})
