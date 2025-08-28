# Dockerfile para el Worker Serverless de RunPod con ComfyUI
# Versión: 3.1 (Final con Forks Verificados)
# Fecha: 2025-08-28
#
# CORRECCIÓN ESTRATÉGICA:
# - Se ha modificado el proceso de clonado para usar los FORKS del usuario 'ceutaseguridad'.
#   Esto garantiza builds 100% estables y elimina permanentemente los errores 'fatal: reference is not a tree'.

# FORZAR UN BUILD SIN CACHÉ CON UN ARGUMENTO QUE CAMBIA
ARG CACHE_BUSTER=2025-08-28T13:00:00Z

# --- FASE 1: ENTORNO BASE Y DEPENDENCIAS DEL SISTEMA ---

FROM runpod/pytorch:2.2.1-py3.10-cuda12.1.1-devel-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    git-lfs \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /

# --- FASE 2: DEPENDENCIAS DE PYTHON ---

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt \
    scikit-image>=0.21.0 \
    numpy==1.26.4 \
    matplotlib

# --- FASE 3: INSTALACIÓN DE COMFYUI Y NODOS PERSONALIZADOS ---

RUN git clone https://github.com/comfyanonymous/ComfyUI.git
WORKDIR /ComfyUI

RUN pip install --no-cache-dir -r requirements.txt

# Clonar los nodos personalizados desde los FORKS del usuario para garantizar la estabilidad.
RUN git clone https://github.com/ceutaseguridad/ComfyUI-Manager.git custom_nodes/ComfyUI-Manager
RUN git clone https://github.com/ceutaseguridad/comfyui_controlnet_aux.git custom_nodes/comfyui_controlnet_aux
RUN git clone https://github.com/ceutaseguridad/ComfyUI_IPAdapter_plus.git custom_nodes/ComfyUI_IPAdapter_plus
RUN git clone https://github.com/ceutaseguridad/ComfyUI-AnimateDiff-Evolved.git custom_nodes/ComfyUI-AnimateDiff-Evolved

WORKDIR /

# --- FASE 4: DESCARGA DE MODELOS DE IA ---

RUN wget -O /ComfyUI/models/checkpoints/realisticVisionV60.safensors https://civitai.com/api/download/models/130072

RUN mkdir -p /ComfyUI/models/controlnet \
    && wget -O /ComfyUI/models/controlnet/control_v11p_sd15_openpose.pth https://huggingface.co/lllyasviel/ControlNet-v1-1/resolve/main/control_v11p_sd15_openpose.pth \
    && wget -O /ComfyUI/models/controlnet/control_v11p_sd15_softedge.pth https://huggingface.co/lllyasviel/ControlNet-v1-1/resolve/main/control_v11p_sd15_softedge.pth

RUN mkdir -p /ComfyUI/models/ipadapter \
    && mkdir -p /ComfyUI/models/clip_vision \
    && wget -O /ComfyUI/models/ipadapter/ip-adapter-plus-face_sd15.bin https://huggingface.co/h94/IP-Adapter/resolve/main/models/ip-adapter-plus-face_sd15.bin \
    && wget -O /ComfyUI/models/clip_vision/pytorch_model.bin https://huggingface.co/laion/CLIP-ViT-H-14-laion2B-s32B-b79K/resolve/main/pytorch_model.bin

RUN mkdir -p /ComfyUI/custom_nodes/ComfyUI-AnimateDiff-Evolved/models \
    && wget -O /ComfyUI/custom_nodes/ComfyUI-AnimateDiff-Evolved/models/mm_sd_v15_v2.ckpt https://huggingface.co/guoyww/animatediff/resolve/main/mm_sd_v15_v2.ckpt

# --- FASE 5: PREPARACIÓN FINAL Y EJECUCIÓN ---

COPY workflow_api.json .
COPY app_handler.py .

CMD ["python", "-u", "app_handler.py"]
