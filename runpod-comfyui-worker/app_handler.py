import runpod
import os
import subprocess
import base64
from typing import Dict, Generator
import json
import uuid
import shutil
import websocket
import time

# --- Constantes y Configuración ---
COMFYUI_PATH = "/ComfyUI"
INPUT_DIR = os.path.join(COMFYUI_PATH, "input")
OUTPUT_DIR = os.path.join(COMFYUI_PATH, "output")
TEMP_DIR = "/tmp/runpod-workspace" # Directorio temporal para nuestros archivos

# --- Inicialización del Servidor ComfyUI ---
# Esta parte se ejecuta una sola vez cuando el worker arranca (cold start)
def start_comfyui_server():
    """Inicia el servidor de ComfyUI en un subproceso."""
    os.chdir(COMFYUI_PATH)
    command = "python main.py --dont-print-server --listen 0.0.0.0 --port 8188"
    print("Iniciando servidor ComfyUI...")
    # Usamos Popen para que no bloquee la ejecución
    server_process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    # Esperar a que el servidor esté listo (una forma simple de hacerlo)
    time.sleep(10) # Darle tiempo para arrancar
    print("Servidor ComfyUI probablemente iniciado.")
    return server_process

server_process = start_comfyui_server()
CLIENT_ID = str(uuid.uuid4())

# --- Funciones de Utilidad ---
def queue_prompt(prompt: Dict, client_id: str) -> Dict:
    """Envía un prompt a la API de ComfyUI."""
    # ... (Implementaremos esto más adelante)
    print(f"Enviando prompt a ComfyUI para el cliente {client_id}")
    return {"prompt_id": "fake_id_123"}

def get_history(prompt_id: str) -> Dict:
    """Obtiene el historial de un prompt ejecutado."""
    # ... (Implementaremos esto más adelante)
    print(f"Obteniendo historial para el prompt {prompt_id}")
    return {}

def get_image_data(filename: str, subfolder: str, folder_type: str) -> bytes:
    """Obtiene los datos de una imagen de la salida de ComfyUI."""
    # ... (Implementaremos esto más adelante)
    print(f"Obteniendo datos de imagen: {filename}")
    return b""

def clean_directory(directory: str):
    """Limpia un directorio."""
    if os.path.exists(directory):
        shutil.rmtree(directory)
    os.makedirs(directory, exist_ok=True)


# --- El Handler Principal de RunPod ---
def handler(job: Dict) -> Dict:
    """
    Función principal que se ejecuta con cada solicitud de trabajo.
    """
    job_input = job['input']
    
    # Validar entradas (a implementar)
    if not all(k in job_input for k in ['base_video_b64', 'face_image_b64', 'prompt']):
        return {"error": "Faltan entradas requeridas: 'base_video_b64', 'face_image_b64', 'prompt'"}

    # Limpiar directorios de trabajos anteriores
    clean_directory(TEMP_DIR)
    clean_directory(INPUT_DIR)
    clean_directory(OUTPUT_DIR)
    
    # 1. Decodificar y guardar archivos de entrada
    print("Decodificando archivos de entrada...")
    try:
        video_path = os.path.join(TEMP_DIR, "base_video.mp4")
        with open(video_path, "wb") as f:
            f.write(base64.b64decode(job_input['base_video_b64']))

        face_image_path = os.path.join(INPUT_DIR, "face_reference.png") # Lo ponemos en INPUT para que ComfyUI lo vea
        with open(face_image_path, "wb") as f:
            f.write(base64.b64decode(job_input['face_image_b64']))
    except Exception as e:
        return {"error": f"Error decodificando archivos: {e}"}

    # 2. Pre-Producción: Descomponer vídeo y extraer audio
    print("Iniciando pre-producción...")
    # ... (Implementaremos la lógica de FFmpeg aquí)
    audio_path = os.path.join(TEMP_DIR, "original_audio.aac")
    frames_dir = os.path.join(INPUT_DIR, "video_frames") # Frames en INPUT para que ComfyUI los cargue
    os.makedirs(frames_dir, exist_ok=True)
    
    # Simulación por ahora
    print(f"Simulando: Vídeo guardado en {video_path}")
    print(f"Simulando: Imagen de cara guardada en {face_image_path}")
    print(f"Simulando: Frames se extraerían a {frames_dir}")

    # 3. Ejecución del Workflow de ComfyUI
    print("Cargando y ejecutando workflow de ComfyUI...")
    # ... (Implementaremos la carga del JSON y la ejecución aquí)
    # Por ahora, simulamos una salida
    output_frames_dir = os.path.join(OUTPUT_DIR, "result_frames")
    os.makedirs(output_frames_dir, exist_ok=True)
    # Copiamos la imagen de referencia como si fuera un frame de salida para probar
    shutil.copy(face_image_path, os.path.join(output_frames_dir, "frame_0001.png"))

    # 4. Post-Producción: Ensamblar vídeo y añadir audio
    print("Iniciando post-producción...")
    # ... (Implementaremos la lógica de FFmpeg aquí)
    final_video_path = os.path.join(TEMP_DIR, "final_video.mp4")
    # Simulación por ahora
    shutil.copy(video_path, final_video_path) # Copiamos el original como resultado de prueba

    # 5. Devolver el resultado
    print("Codificando y devolviendo el resultado...")
    try:
        with open(final_video_path, "rb") as f:
            final_video_b64 = base64.b64encode(f.read()).decode('utf-8')
        
        # Limpieza final
        shutil.rmtree(TEMP_DIR)

        return {"video_b64": final_video_b64}
    except Exception as e:
        return {"error": f"Error codificando el resultado: {e}"}

# --- Iniciar el servidor de RunPod ---
if __name__ == "__main__":
    print("Iniciando worker de ComfyUI para RunPod...")
    runpod.serverless.start({"handler": handler})