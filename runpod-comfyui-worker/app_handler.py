# app_handler.py (versión actualizada)

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
import requests # Añadir requests para descargar desde S3

# --- (Las constantes y funciones de ComfyUI no cambian) ---
COMFYUI_URL = "http://127.0.0.1:8188"
COMFYUI_PATH = "/ComfyUI"
INPUT_DIR = os.path.join(COMFYUI_PATH, "input")
OUTPUT_DIR = os.path.join(COMFYUI_PATH, "output")
TEMP_DIR = "/tmp/runpod-workspace"

def start_comfyui_server():
    # ... (sin cambios)
def queue_prompt(prompt: Dict, client_id: str) -> Dict:
    # ... (sin cambios)
def get_history(prompt_id: str) -> Dict:
    # ... (sin cambios)
def get_image(filename, subfolder, folder_type):
    # ... (sin cambios)
def clean_directory(directory: str):
    # ... (sin cambios)
def process_video_with_comfyui(frames_input_dir: str, face_ref_path: str, prompt_text: str, workflow_json_path: str) -> str:
    # ... (sin cambios)

# --- El Handler Principal de RunPod (MODIFICADO) ---
def handler(job: Dict) -> Dict:
    job_input = job['input']
    
    # NUEVA ESTRUCTURA DE INPUT: Esperamos URLs en lugar de Base64
    if not all(k in job_input for k in ['base_video_url', 'face_image_url', 'prompt']):
        return {"error": "Faltan entradas requeridas: 'base_video_url', 'face_image_url', 'prompt'"}

    clean_directory(TEMP_DIR)
    clean_directory(INPUT_DIR)
    clean_directory(OUTPUT_DIR)
    
    # 1. Descargar archivos de entrada desde las URLs S3
    print("Descargando archivos de entrada desde S3...")
    try:
        video_path = os.path.join(TEMP_DIR, "base_video.mp4")
        response_video = requests.get(job_input['base_video_url'], stream=True)
        response_video.raise_for_status()
        with open(video_path, "wb") as f:
            for chunk in response_video.iter_content(chunk_size=8192):
                f.write(chunk)
        
        face_image_path = os.path.join(INPUT_DIR, "face_reference.png")
        response_face = requests.get(job_input['face_image_url'], stream=True)
        response_face.raise_for_status()
        with open(face_image_path, "wb") as f:
            for chunk in response_face.iter_content(chunk_size=8192):
                f.write(chunk)

    except Exception as e:
        return {"error": f"Error descargando archivos desde S3: {e}"}

    # --- El resto del proceso es idéntico ---
    # 2. Pre-Producción
    audio_path = os.path.join(TEMP_DIR, "original_audio.aac")
    frames_input_dir = os.path.join(INPUT_DIR, "video_frames")
    os.makedirs(frames_input_dir, exist_ok=True)
    try:
        subprocess.run(f"ffmpeg -i {video_path} -vn -acodec copy {audio_path}", shell=True, check=True)
        subprocess.run(f"ffmpeg -i {video_path} {os.path.join(frames_input_dir, 'frame_%04d.png')}", shell=True, check=True)
    except subprocess.CalledProcessError as e: return {"error": f"FFmpeg pre-producción falló: {e}"}

    # 3. Ejecución del Workflow
    try:
        process_video_with_comfyui(frames_input_dir, face_image_path, job_input['prompt'], "/workflow_api.json")
        print("Esperando a que ComfyUI genere los frames de salida...")
        output_frames_path = OUTPUT_DIR
        timeout = 3600 # Aumentar timeout a 1 hora
        start_time = time.time()
        files_found = False
        while time.time() - start_time < timeout:
            if any(f.endswith('.png') for f in os.listdir(output_frames_path)):
                print("Frames de salida detectados.")
                files_found = True
                break
            time.sleep(5)
        if not files_found: return {"error": "Timeout: ComfyUI no generó los frames de salida a tiempo."}
    except Exception as e: return {"error": f"Error en el workflow de ComfyUI: {e}"}

    # 4. Post-Producción
    final_video_path = os.path.join(TEMP_DIR, "final_video.mp4")
    try:
        subprocess.run(f"ffmpeg -framerate 24 -i {os.path.join(output_frames_path, 'ResultFrames_%05d.png')} -c:v libx264 -pix_fmt yuv420p {os.path.join(TEMP_DIR, 'video_no_audio.mp4')}", shell=True, check=True)
        subprocess.run(f"ffmpeg -i {os.path.join(TEMP_DIR, 'video_no_audio.mp4')} -i {audio_path} -c:v copy -c:a aac -shortest {final_video_path}", shell=True, check=True)
    except subprocess.CalledProcessError as e: return {"error": f"FFmpeg post-producción falló: {e}"}

    # 5. Devolver resultado (por ahora, seguimos usando Base64 para la salida)
    try:
        with open(final_video_path, "rb") as f:
            video_b64 = base64.b64encode(f.read()).decode('utf-8')
        shutil.rmtree(TEMP_DIR)
        return {"video_b64": video_b64}
    except Exception as e: return {"error": f"Error codificando el resultado: {e}"}


# --- Iniciar el servidor de RunPod ---
if __name__ == "__main__":
    # ... (sin cambios)
