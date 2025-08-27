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
def get_images(ws: websocket.WebSocket, prompt: Dict) -> Generator[bytes, None, None]:
    prompt_id = queue_prompt(prompt, CLIENT_ID)['prompt_id']
    while True:
        out = ws.recv()
        if isinstance(out, str):
            message = json.loads(out)
            if message['type'] == 'executing':
                data = message['data']
                if data['node'] is None and data['prompt_id'] == prompt_id:
                    break # Execution is done
        else:
            continue # previews are binary data

    history = get_history(prompt_id)[prompt_id]
    for node_id in history['outputs']:
        node_output = history['outputs'][node_id]
        if 'images' in node_output:
            for image in node_output['images']:
                image_data = get_image(image['filename'], image['subfolder'], image['type'])
                yield image_data

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
def process_video_with_comfyui(frames_input_dir: str, face_ref_path: str, prompt_text: str, workflow_json_path: str) -> str:
    """Ejecuta el workflow de ComfyUI para un conjunto de frames."""
    ws = websocket.WebSocket()
    ws.connect(f"ws://{COMFYUI_URL}/ws?clientId={CLIENT_ID}")
    
    with open(workflow_json_path, 'r') as f:
        prompt_workflow = json.load(f)

    # Modificar el workflow dinámicamente
    prompt_workflow["4"]["inputs"]["text"] = prompt_text # Positive prompt
    prompt_workflow["6"]["inputs"]["image"] = os.path.basename(face_ref_path) # Face reference
    prompt_workflow["7"]["inputs"]["directory"] = os.path.basename(frames_input_dir) # Video frames folder

    # La salida de get_images no es necesaria para vídeo, solo ejecutamos el prompt.
    # El nodo SaveImage de ComfyUI guardará los archivos en el disco del worker.
    queue_prompt(prompt_workflow, CLIENT_ID)
    
    # Esperar a que la ejecución termine (simplificado)
    # Una solución robusta monitorizaría el progreso, aquí esperamos un tiempo estimado o buscamos los archivos
    # Por ahora, confiamos en que el nodo SaveImage haga su trabajo.
    # El verdadero 'get_images' está implícito en que los archivos aparecerán en OUTPUT_DIR.
    # Este es un punto a mejorar con un monitoreo más activo si fuera necesario.

    ws.close()
    return OUTPUT_DIR # Devolvemos la carpeta donde se guardan los resultados

# --- El Handler Principal de RunPod ---
def handler(job: Dict) -> Dict:
    job_input = job['input']
    if not all(k in job_input for k in ['base_video_b64', 'face_image_b64', 'prompt']):
        return {"error": "Faltan entradas requeridas."}

    clean_directory(TEMP_DIR)
    clean_directory(INPUT_DIR)
    clean_directory(OUTPUT_DIR)
    
    # 1. Decodificar y guardar
    try:
        video_path = os.path.join(TEMP_DIR, "base_video.mp4")
        with open(video_path, "wb") as f: f.write(base64.b64decode(job_input['base_video_b64']))
        
        # El nombre del archivo debe coincidir con el del workflow JSON
        face_image_path = os.path.join(INPUT_DIR, "face_reference.png")
        with open(face_image_path, "wb") as f: f.write(base64.b64decode(job_input['face_image_b64']))
    except Exception as e: return {"error": f"Error decodificando archivos: {e}"}

    # 2. Pre-Producción
    audio_path = os.path.join(TEMP_DIR, "original_audio.aac")
    # El nombre de la carpeta debe coincidir con el del workflow JSON
    frames_input_dir = os.path.join(INPUT_DIR, "video_frames")
    os.makedirs(frames_input_dir, exist_ok=True)
    
    try:
        # Extraer audio
        subprocess.run(f"ffmpeg -i {video_path} -vn -acodec copy {audio_path}", shell=True, check=True)
        # Extraer frames
        subprocess.run(f"ffmpeg -i {video_path} {os.path.join(frames_input_dir, 'frame_%04d.png')}", shell=True, check=True)
    except subprocess.CalledProcessError as e: return {"error": f"FFmpeg pre-producción falló: {e}"}

    # 3. Ejecución del Workflow
    try:
        process_video_with_comfyui(frames_input_dir, face_image_path, job_input['prompt'], "/workflow_api.json")
    
        # Espera activa a que se generen los archivos de salida
        print("Esperando a que ComfyUI genere los frames de salida...")
        # Lógica de espera simple: revisar cada 5s si hay archivos en output
        output_frames_path = OUTPUT_DIR
        timeout = 300 # 5 minutos de timeout
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

    except Exception as e: return {"error": f"Error en el workflow de ComfyUI: {e}"}

    # 4. Post-Producción
    final_video_path = os.path.join(TEMP_DIR, "final_video.mp4")
    try:
        # Recomponer vídeo desde frames
        # Usamos -framerate 24, asumiendo que el original era a 24fps. Esto debería ser un parámetro.
        subprocess.run(f"ffmpeg -framerate 24 -i {os.path.join(output_frames_path, 'ResultFrames_%05d.png')} -c:v libx264 -pix_fmt yuv420p {os.path.join(TEMP_DIR, 'video_no_audio.mp4')}", shell=True, check=True)
        # Fusionar con audio
        subprocess.run(f"ffmpeg -i {os.path.join(TEMP_DIR, 'video_no_audio.mp4')} -i {audio_path} -c:v copy -c:a aac -shortest {final_video_path}", shell=True, check=True)
    except subprocess.CalledProcessError as e: return {"error": f"FFmpeg post-producción falló: {e}"}

    # 5. Devolver resultado
    try:
        with open(final_video_path, "rb") as f:
            video_b64 = base64.b64encode(f.read()).decode('utf-8')
        
        shutil.rmtree(TEMP_DIR)
        return {"video_b64": video_b64}
    except Exception as e: return {"error": f"Error codificando el resultado: {e}"}


# --- Iniciar el servidor de RunPod ---
if __name__ == "__main__":
    # Necesitamos copiar el workflow al directorio raíz para que el handler lo encuentre
    shutil.copyfile(os.path.join(COMFYUI_PATH, "../workflow_api.json"), "/workflow_api.json")
    print("Iniciando worker de ComfyUI para RunPod...")
    runpod.serverless.start({"handler": handler})