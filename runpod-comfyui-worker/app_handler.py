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
from flask import Flask, request, jsonify

# --- RUTAS ---
WORKSPACE_DIR = "/workspace"
COMFYUI_PATH = os.path.join(WORKSPACE_DIR, "ComfyUI")
INPUT_DIR = os.path.join(COMFYUI_PATH, "input")
OUTPUT_DIR = os.path.join(COMFYUI_PATH, "output")
TEMP_DIR = "/tmp/runpod-handler-temp" # Usamos un directorio temporal aislado

# --- INICIALIZACIÓN GLOBAL ---
CLIENT_ID = str(uuid.uuid4())
COMFYUI_API_URL = "http://127.0.0.1:8188"

# --- LÓGICA DE PROCESAMIENTO (EL MOTOR) ---

def queue_prompt(prompt: Dict[str, Any], client_id: str) -> Dict[str, Any]:
    """Envía un workflow a la API de ComfyUI para que lo ponga en cola."""
    try:
        p = {"prompt": prompt, "client_id": client_id}
        data = json.dumps(p).encode('utf-8')
        req = urllib.request.Request(f"{COMFYUI_API_URL}/prompt", data=data)
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read())
    except Exception as e:
        # Devuelve un diccionario de error compatible con el resto del flujo
        return {"error": f"Failed to queue prompt: {str(e)}"}


def get_history(prompt_id: str) -> Dict[str, Any]:
    """Consulta el historial de un prompt para ver si ha terminado."""
    try:
        with urllib.request.urlopen(f"{COMFYUI_API_URL}/history/{prompt_id}") as response:
            return json.loads(response.read())
    except Exception:
        # Si hay un error de conexión, devolvemos un dict vacío para no romper el bucle
        return {}

def clean_directory(directory: str):
    """Limpia un directorio de forma segura."""
    if os.path.exists(directory):
        shutil.rmtree(directory)
    os.makedirs(directory, exist_ok=True)
    
def find_output_video(prompt_id: str, history: Dict[str, Any]) -> str | None:
    """Busca en el historial la ruta del archivo de vídeo generado."""
    outputs = history.get(prompt_id, {}).get('outputs', {})
    for node_id, node_output in outputs.items():
        if 'videos' in node_output:
            for video_info in node_output['videos']:
                if video_info.get('type') == 'output':
                    filename = video_info.get('filename')
                    # La ruta completa es subdirectorio/nombre
                    full_path = os.path.join(OUTPUT_DIR, video_info.get('subfolder', ''), filename)
                    return full_path
    return None


def handler(job: Dict[str, Any]) -> Dict[str, Any]:
    job_input = job.get('input', {})
    video_b64 = job_input.get('video_b64')
    face_b64 = job_input.get('face_b64')
    prompt_text = job_input.get('prompt', '')

    if not all([video_b64, face_b64, prompt_text]):
        return {"error": "Missing 'video_b64', 'face_b64', or 'prompt' in job input."}

    # Limpieza inicial de directorios
    clean_directory(INPUT_DIR)
    clean_directory(OUTPUT_DIR)
    clean_directory(TEMP_DIR)

    # Nombres de archivo fijos que coinciden con el workflow_api.json
    video_filename = "input_video.mp4"
    face_filename = "face_reference.png"
    
    video_path_in_comfyui = os.path.join(INPUT_DIR, video_filename)
    face_path_in_comfyui = os.path.join(INPUT_DIR, face_filename)

    try:
        print("1/5 - Decoding Base64 input files...", flush=True)
        with open(video_path_in_comfyui, "wb") as f: f.write(base64.b64decode(video_b64))
        with open(face_path_in_comfyui, "wb") as f: f.write(base64.b64decode(face_b64))
        print("Input files successfully placed in ComfyUI's input directory.", flush=True)
    except Exception as e:
        return {"error": f"Error decoding or preparing input files: {str(e)}"}

    # --- LA EXTRACCIÓN DE FRAMES CON FFMPEG HA SIDO ELIMINADA ---
    # El nodo VHS_LoadVideo se encarga de esto internamente.
    
    # --- LA RECOMBINACIÓN DE VÍDEO CON FFMPEG HA SIDO ELIMINADA ---
    # El nodo VHS_VideoCombine se encargará de esto.

    try:
        print("2/5 - Loading workflow from disk...", flush=True)
        workflow_path = os.path.join(WORKSPACE_DIR, "workflow_api.json")
        with open(workflow_path, 'r') as f:
            prompt_workflow = json.load(f)

        print("3/5 - Injecting dynamic data into workflow...", flush=True)
        # Inyectamos el prompt del usuario en el nodo de prompt positivo (asumimos que es el nodo #4)
        prompt_workflow["4"]["inputs"]["text"] = prompt_text
        
        # Opcional: Asegurarnos de que los nombres de archivo coinciden
        prompt_workflow["6"]["inputs"]["image"] = face_filename
        prompt_workflow["7"]["inputs"]["video"] = video_filename

        print("4/5 - Queueing workflow to ComfyUI...", flush=True)
        response = queue_prompt(prompt_workflow, CLIENT_ID)
        
        if 'error' in response:
            return {"error": f"API Error when queueing prompt: {response['error']}"}
        
        if 'prompt_id' not in response:
            raise RuntimeError(f"Failed to queue prompt. API response: {response}")
        
        prompt_id = response['prompt_id']
        print(f"Workflow queued with Prompt ID: {prompt_id}", flush=True)

        timeout = 7200  # 2 horas
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            history = get_history(prompt_id)
            if prompt_id in history and history[prompt_id].get('outputs'):
                print("Workflow completed.", flush=True)
                break
            
            elapsed = int(time.time() - start_time)
            print(f"Waiting for ComfyUI... ({elapsed}s)", flush=True)
            time.sleep(10)
        else:
            return {"error": "Timeout: ComfyUI did not complete the workflow in time."}

    except Exception as e:
        return {"error": f"ComfyUI workflow execution error: {str(e)}"}

    try:
        print("5/5 - Finding and encoding final video...", flush=True)
        final_video_path = find_output_video(prompt_id, history)
        
        if not final_video_path or not os.path.exists(final_video_path):
            return {"error": "Processing finished, but the final video file was not found in ComfyUI's output."}

        with open(final_video_path, "rb") as f:
            final_video_b64 = base64.b64encode(f.read()).decode('utf-8')
        
        print("Successfully encoded final video. Job complete.", flush=True)
        return {"video_b64": final_video_b64}
    
    except Exception as e:
        return {"error": f"Error encoding the result: {str(e)}"}
    finally:
        # Limpieza final para no acumular archivos entre ejecuciones
        clean_directory(INPUT_DIR)
        clean_directory(OUTPUT_DIR)
        clean_directory(TEMP_DIR)


# --- SERVIDOR WEB (LA PUERTA DE ENTRADA) ---
app = Flask(__name__)

@app.route('/run', methods=['POST'])
def run_sync():
    print("\n--- Received new job request on /run ---", flush=True)
    job_payload = request.get_json()
    result = handler(job_payload)
    print("--- Handler processing finished. Sending response. ---\n", flush=True)
    return jsonify(result)

if __name__ == "__main__":
    print("[HANDLER] Starting Flask server...", flush=True)
    app.run(host='0.0.0.0', port=8888)
