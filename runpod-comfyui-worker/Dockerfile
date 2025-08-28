# Dockerfile para el Worker Serverless de RunPod con ComfyUI
# Versión: 2.1 (Base Image Corregida)
# Fecha: 2025-08-28
#
# CORRECCIÓN CRÍTICA:
# - Se ha cambiado la imagen base en la instrucción FROM a una que existe y está verificada en el Docker Hub de RunPod.
#   Esto resuelve el error "not found" durante el proceso de build. La nueva imagen utiliza PyTorch 2.3.0, CUDA 12.1.1 y Python 3.11.

# FORZAR UN BUILD SIN CACHÉ CON UN ARGUMENTO QUE CAMBIA
ARG CACHE_BUSTER=2025-08-28T09:00:00Z

# --- FASE 1: ENTORNO BASE Y DEPENDENCIAS DEL SISTEMA ---

# CORRECCIÓN: Se utiliza un tag de imagen oficial y existente de RunPod.
FROM runpod/pytorch:2.3.0-py3.11-cuda12.1.1-devel-ubuntu22.04

# Variables de entorno para evitar preguntas interactivas durante la construcción
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC

# Instalar dependencias del sistema: git, git-lfs para modelos grandes, y ffmpeg para vídeo/audio.
# Se agrupan en una sola capa RUN para mayor eficiencia.
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    git-lfs \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Establecer el directorio de trabajo principal
WORKDIR /

# --- FASE 2: DEPENDENCIAS DE PYTHON ---

# Copiar el archivo de requisitos
COPY requirements.txt .

# Instalar dependencias de Python del handler y las correcciones necesarias.
# Se agrupan todas las instalaciones de pip en una sola capa.
RUN pip install --no-cache-dir -r requirements.txt \
    # [CORRECCIÓN CRÍTICA] Instalar la dependencia faltante para comfyui_controlnet_aux (DWPose, etc.)
    scikit-image>=0.21.0 \
    # [CORRECCIÓN PREVENTIVA] Fijar la versión de NumPy a <2.0 para evitar conflictos de compatibilidad.
    numpy==1.26.4 \
    # [CORRECCIÓN] Instalar dependencia no declarada de comfyui_controlnet_aux.
    matplotlib

# --- FASE 3: INSTALACIÓN DE COMFYUI Y NODOS PERSONALIZADOS ---

# Clonar el repositorio principal de ComfyUI
RUN git clone https://github.com/comfyanonymous/ComfyUI.git
WORKDIR /ComfyUI

# Instalar dependencias específicas de ComfyUI
RUN pip install --no-cache-dir -r requirements.txt

# Clonar los nodos personalizados, fijando cada uno a un commit específico para asegurar la reproducibilidad del build.
# Esto previene que una actualización en el repositorio original rompa nuestro workflow.
RUN git clone https://github.com/ltdrdata/ComfyUI-Manager.git custom_nodes/ComfyUI-Manager \
    && cd custom_nodes/ComfyUI-Manager && git checkout 2b584045a1f68153b67e7ed4882155734268e6f1 && cd ../../

RUN git clone https://github.com/Fannovel16/comfyui_controlnet_aux.git custom_nodes/comfyui_controlnet_aux \
    && cd custom_nodes/comfyui_controlnet_aux && git checkout 6f53435165842817816e8140445d43e3c75d496a && cd ../../

RUN git clone https://github.com/cubiq/ComfyUI_IPAdapter_plus.git custom_nodes/ComfyUI_IPAdapter_plus \
    && cd custom_nodes/ComfyUI_IPAdapter_plus && git checkout 8d7b3401fa9b4703a110a3014f315a6b093551eb && cd ../../

RUN git clone https://github.com/Kosinkadink/ComfyUI-AnimateDiff-Evolved.git custom_nodes/ComfyUI-AnimateDiff-Evolved \
    && cd custom_nodes/ComfyUI-AnimateDiff-Evolved && git checkout 571d7ca27339d677b81bfb904ed4a36f6d0f10c6 && cd ../../

# Volver al directorio raíz para las siguientes operaciones
WORKDIR /

# --- FASE 4: DESCARGA DE MODELOS DE IA ---
# Los modelos se descargan durante la construcción para acelerar drásticamente los "cold starts".

# 1. Checkpoint Fotorrealista (Modelo Base de Stable Diffusion)
RUN wget -O /ComfyUI/models/checkpoints/realisticVisionV60.safetensors https://civitai.com/api/download/models/130072

# 2. Modelos de ControlNet (Guía de Estructura)
RUN mkdir -p /ComfyUI/models/controlnet \
    && wget -O /ComfyUI/models/controlnet/control_v11p_sd15_openpose.pth https://huggingface.co/lllyasviel/ControlNet-v1-1/resolve/main/control_v11p_sd15_openpose.pth \
    && wget -O /ComfyUI/models/controlnet/control_v11p_sd15_softedge.pth https://huggingface.co/lllyasviel/ControlNet-v1-1/resolve/main/control_v11p_sd15_softedge.pth

# 3. Modelos de IP-Adapter (Guía de Identidad Facial)
RUN mkdir -p /ComfyUI/models/ipadapter \
    && mkdir -p /ComfyUI/models/clip_vision \
    && wget -O /ComfyUI/models/ipadapter/ip-adapter-plus-face_sd15.bin https://huggingface.co/h94/IP-Adapter/resolve/main/models/ip-adapter-plus-face_sd15.bin \
    # CLIP Vision Encoder
    && wget -O /ComfyUI/models/clip_vision/pytorch_model.bin https://huggingface.co/laion/CLIP-ViT-H-14-laion2B-s32B-b79K/resolve/main/pytorch_model.bin

# 4. Modelos de AnimateDiff (Guía de Movimiento y Consistencia Temporal)
RUN mkdir -p /ComfyUI/custom_nodes/ComfyUI-AnimateDiff-Evolved/models \
    && wget -O /ComfyUI/custom_nodes/ComfyUI-AnimateDiff-Evolved/models/mm_sd_v15_v2.ckpt https://huggingface.co/guoyww/animatediff/resolve/main/mm_sd_v15_v2.ckpt

# --- FASE 5: PREPARACIÓN FINAL Y EJECUCIÓN ---

# Copiar el workflow y nuestro script handler a la raíz del contenedor
COPY workflow_api.json .
COPY app_handler.py .

# Comando que se ejecuta cuando el worker serverless se inicia.
# El flag "-u" fuerza a que la salida de Python no se almacene en búfer,
# lo que es crucial para ver los logs en tiempo real en RunPod.
CMD ["python", "-u", "app_handler.py"]
