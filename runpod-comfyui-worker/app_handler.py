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
# Se elimina 'requests' ya que no es necesario para descargar

# --- Constantes y Configuración ---
COMFYUI_URL = "http://127.0.0.1:8188"
COMFYUI_PATH = "/ComfyUI"
INPUT_DIR = os.path.join(COMFYUI_PATH, "input")
OUTPUT_DIR = os.path.join(COMFYUI_PATH, "output")
TEMP_DIR = "/tmp/runpod-workspace" # Directorio temporal para audio y video final

# --- Inicialización del Servidor ComfyUI (sin cambios) ---
def start_comfyui_server():
    os.chdir(COMFYUI_PATH)
    command = "python main.py --dont-print-server --listen 0.0.0.0 --port 8188"
    server_process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
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

# --- Funciones de Comunicación con ComfyUI (sin cambios) ---
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

# --- Funciones de Procesamiento de Vídeo con ComfyUI (sin cambios) ---
def process_video_with_comfyui(frames_input_dir: str, face_ref_path: str, prompt_text: str, workflow_json_path: str):
    ws = websocket.WebSocket()
    ws.connect(f"ws://{COMFYUI_URL}/ws?clientId={CLIENT_ID}")
    
    with open(workflow_json_path, 'r') as f:
        prompt_workflow = json.load(f)

    # Inyectar los datos en el workflow
    prompt_workflow["4"]["inputs"]["text"] = prompt_text
    prompt_workflow["6"]["inputs"]["image"] = os.path.basename(face_ref_path) # ComfyUI necesita el nombre base del archivo
    prompt_workflow["7"]["inputs"]["directory"] = frames_input_dir # ComfyUI necesita la ruta completa del directorio de frames

    queue_prompt(prompt_workflow, CLIENT_ID)
    ws.close()

# --- El Handler Principal de RunPod (CORREGIDO) ---
def handler(job: Dict) -> Dict:
    job_input = job['input']
    
    # CORRECCIÓN 1: Buscar las claves correctas que envía app.py
    expected_keys = ['video_filename', 'face_filename', 'prompt']
    if not all(k in job_input for k in expected_keys):
        return {"error": f"Faltan entradas requeridas. Se esperaban: {expected_keys}, se recibieron: {list(job_input.keys())}"}

    # Limpiar directorios de trabajos anteriores
    clean_directory(TEMP_DIR)
    clean_directory(INPUT_DIR)
    clean_directory(OUTPUT_DIR)
    
    # CORRECCIÓN 2.1: Buscar los archivos en la ruta correcta.
print("Localizando archivos de entrada (RunPod ya los ha descargado)...")
try:
    video_filename = job_input['video_filename']
    face_filename = job_input['face_filename']
    
    # RunPod descarga los archivos en la raíz del volumen, no en el directorio de trabajo.
    video_path_on_disk = os.path.join("/", video_filename)
    face_path_on_disk = os.path.join("/", face_filename)
    
    # Para depuración, vamos a listar los contenidos de la raíz.
    print(f"Buscando archivos en la raíz. Contenido de '/': {os.listdir('/')}")
    print(f"Directorio de trabajo actual (getcwd): {os.getcwd()}")
    
    if not os.path.exists(video_path_on_disk) or not os.path.exists(face_path_on_disk):
        # Devolvemos un error aún más detallado.
        return {"error": f"Los archivos de entrada no se encontraron. Buscando en '{video_path_on_disk}' y '{face_path_on_disk}'. Contenido real de la raíz: {os.listdir('/')}"}

    # ComfyUI necesita los archivos en su propio directorio de 'input'
    # Copiamos la imagen de la cara allí
    face_image_path_in_comfyui = os.path.join(INPUT_DIR, face_filename)
    shutil.copy(face_path_on_disk, face_image_path_in_comfyui)
    print(f"Éxito: Vídeo base encontrado en: {video_path_on_disk}")
    print(f"Éxito: Imagen de cara copiada a: {face_image_path_in_comfyui}")

except Exception as e:
    return {"error": f"Error preparando los archivos de entrada: {e}"}

    # 2. Pre-Producción (ahora usa la ruta correcta del vídeo)
    print("Iniciando pre-producción (extrayendo frames y audio)...")
    audio_path = os.path.join(TEMP_DIR, "original_audio.aac")
    frames_input_dir = os.path.join(INPUT_DIR, "video_frames")
    os.makedirs(frames_input_dir, exist_ok=True)
    try:
        # Usamos video_path_on_disk, la ruta local correcta
        subprocess.run(f"ffmpeg -i {video_path_on_disk} -vn -acodec copy {audio_path}", shell=True, check=True, capture_output=True, text=True)
        subprocess.run(f"ffmpeg -i {video_path_on_disk} {os.path.join(frames_input_dir, 'frame_%04d.png')}", shell=True, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        return {"error": f"FFmpeg pre-producción falló: {e.stderr}"}

    # 3. Ejecución del Workflow (ahora usa la ruta correcta de la cara)
    print("Ejecutando workflow de ComfyUI...")
    try:
        # Usamos face_image_path_in_comfyui, la ruta dentro de ComfyUI/input
        process_video_with_comfyui(frames_input_dir, face_image_path_in_comfyui, job_input['prompt'], "/workflow_api.json")
    
        print("Esperando a que ComfyUI genere los frames de salida...")
        output_frames_path = OUTPUT_DIR
        timeout = 3600 # 1 hora
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

    # 4. Post-Producción (sin cambios)
    print("Iniciando post-producción (ensamblando video final)...")
    final_video_path = os.path.join(TEMP_DIR, "final_video.mp4")
    try:
        # CORRECCIÓN 3: Asegurarse de que el patrón de nombre de archivo coincida con la salida real de ComfyUI.
        # Ajusta 'ResultFrames_%05d.png' si tu workflow genera otro nombre.
        ffmpeg_input_pattern = os.path.join(output_frames_path, 'ResultFrames_%05d.png')
        subprocess.run(f"ffmpeg -framerate 24 -i \"{ffmpeg_input_pattern}\" -c:v libx264 -pix_fmt yuv420p {os.path.join(TEMP_DIR, 'video_no_audio.mp4')}", shell=True, check=True, capture_output=True, text=True)
        subprocess.run(f"ffmpeg -i {os.path.join(TEMP_DIR, 'video_no_audio.mp4')} -i {audio_path} -c:v copy -c:a aac -shortest {final_video_path}", shell=True, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        return {"error": f"FFmpeg post-producción falló: {e.stderr}"}

    # 5. Devolver resultado (sin cambios)
    print("Codificando y devolviendo el resultado...")
    try:
        with open(final_video_path, "rb") as f:
            video_b64 = base64.b64encode(f.read()).decode('utf-8')
        
        shutil.rmtree(TEMP_DIR)
        return {"video_b64": video_b64}
    except Exception as e:
        return {"error": f"Error codificando el resultado: {e}"}


# --- Iniciar el servidor de RunPod (sin cambios) ---
if __name__ == "__main__":
    if not os.path.exists("/workflow_api.json"):
        shutil.copyfile(os.path.join(COMFYUI_PATH, "../workflow_api.json"), "/workflow_api.json")
    print("Iniciando worker de ComfyUI para RunPod...")
    runpod.serverless.start({"handler": handler})

