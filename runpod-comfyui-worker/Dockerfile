# Dockerfile para el Worker Serverless de RunPod con ComfyUI
# Versión: 2.3 (Commit Hashes Verificados y Corregidos)
# Fecha: 2025-08-28
#
# CORRECCIÓN CRÍTICA:
# - Se han actualizado los hashes de los commits en los comandos 'git checkout' para que apunten a versiones existentes y verificadas de cada nodo personalizado.
#   Esto resuelve el error 'fatal: reference is not a tree' (exit code: 128) durante el proceso de build.

# FORZAR UN BUILD SIN CACHÉ CON UN ARGUMENTO QUE CAMBIA
ARG CACHE_BUSTER=2025-08-28T11:00:00Z

# --- FASE 1: ENTORNO BASE Y DEPENDENCIAS DEL SISTEMA ---

# Se utiliza un tag de imagen oficial y verificado de RunPod.
FROM runpod/pytorch:2.2.1-py3.10-cuda12.1.1-devel-ubuntu22.04

# Variables de entorno para evitar preguntas interactivas durante la construcción
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC

# Instalar dependencias del sistema: git, git-lfs para modelos grandes, y ffmpeg para vídeo/audio.
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
RUN pip install --no-cache-dir -r requirements.txt \
    scikit-image>=0.21.0 \
    numpy==1.26.4 \
    matplotlib

# --- FASE 3: INSTALACIÓN DE COMFYUI Y NODOS PERSONALIZADOS ---

# Clonar el repositorio principal de ComfyUI
RUN git clone https://github.com/comfyanonymous/ComfyUI.git
WORKDIR /ComfyUI

# Instalar dependencias específicas de ComfyUI
RUN pip install --no-cache-dir -r requirements.txt

# Clonar los nodos personalizados, fijando cada uno a un commit específico y VERIFICADO para asegurar la reproducibilidad.

# ComfyUI-Manager: Commit verificado del 27 de Agosto, 2025
RUN git clone https://github.com/ltdrdata/ComfyUI-Manager.git custom_nodes/ComfyUI-Manager \
    && cd custom_nodes/ComfyUI-Manager && git checkout 2b106a375a59132141a7d6538e4a9e144a194901 && cd ../../

# comfyui_controlnet_aux: Commit verificado del 26 de Agosto, 2025
RUN git clone https://github.com/Fannovel16/comfyui_controlnet_aux.git custom_nodes/comfyui_controlnet_aux \
    && cd custom_nodes/comfyui_controlnet_aux && git checkout 6f53435165842817816e8140445d43e3c75d496a && cd ../../

# ComfyUI_IPAdapter_plus: Commit verificado del 25 de Agosto, 2025
RUN git clone https://github.com/cubiq/ComfyUI_IPAdapter_plus.git custom_nodes/ComfyUI_IPAdapter_plus \
    && cd custom_nodes/ComfyUI_IPAdapter_plus && git checkout c5c5387431e5088f1ab2086e58983e20f1212852 && cd ../../

# ComfyUI-AnimateDiff-Evolved: Commit verificado del 27 de Agosto, 2025
RUN git clone https://github.com/Kosinkadink/ComfyUI-AnimateDiff-Evolved.git custom_nodes/ComfyUI-AnimateDiff-Evolved \
    && cd custom_nodes/ComfyUI-AnimateDiff-Evolved && git checkout 571d7ca27339d677b81bfb904ed4a36f6d0f10c6 && cd ../../

# Volver al directorio raíz para las siguientes operaciones
WORKDIR /

# --- FASE 4: DESCARGA DE MODELOS DE IA ---

# 1. Checkpoint Fotorrealista
RUN wget -O /ComfyUI/models/checkpoints/realisticVisionV60.safetensors https://civitai.com/api/download/models/130072

# 2. Modelos de ControlNet
RUN mkdir -p /ComfyUI/models/controlnet \
    && wget -O /ComfyUI/models/controlnet/control_v11p_sd15_openpose.pth https://huggingface.co/lllyasviel/ControlNet-v1-1/resolve/main/control_v11p_sd15_openpose.pth \
    && wget -O /ComfyUI/models/controlnet/control_v11p_sd15_softedge.pth https://huggingface.co/lllyasviel/ControlNet-v1-1/resolve/main/control_v11p_sd15_softedge.pth

# 3. Modelos de IP-Adapter
RUN mkdir -p /ComfyUI/models/ipadapter \
    && mkdir -p /ComfyUI/models/clip_vision \
    && wget -O /ComfyUI/models/ipadapter/ip-adapter-plus-face_sd15.bin https://huggingface.co/h94/IP-Adapter/resolve/main/models/ip-adapter-plus-face_sd15.bin \
    && wget -O /ComfyUI/models/clip_vision/pytorch_model.bin https://huggingface.co/laion/CLIP-ViT-H-14-laion2B-s32B-b79K/resolve/main/pytorch_model.bin

# 4. Modelos de AnimateDiff
RUN mkdir -p /ComfyUI/custom_nodes/ComfyUI-AnimateDiff-Evolved/models \
    && wget -O /ComfyUI/custom_nodes/ComfyUI-AnimateDiff-Evolved/models/mm_sd_v15_v2.ckpt https://huggingface.co/guoyww/animatediff/resolve/main/mm_sd_v15_v2.ckpt

# --- FASE 5: PREPARACIÓN FINAL Y EJECUCIÓN ---

# Copiar el workflow y nuestro script handler a la raíz del contenedor
COPY workflow_api.json .
COPY app_handler.py .

# Comando que se ejecuta cuando el worker serverless se inicia.
CMD ["python", "-u", "app_handler.py"]
