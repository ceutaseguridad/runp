# Usamos la imagen oficial de RunPod con Pytorch y CUDA preinstalados.
FROM runpod/pytorch:2.1.0-py3.10-cuda11.8.0-devel

# Variables de entorno para evitar preguntas interactivas
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

# Instalar las dependencias de Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- Instalación de ComfyUI y Custom Nodes ---
WORKDIR /
RUN git clone https://github.com/comfyanonymous/ComfyUI.git
WORKDIR /ComfyUI
RUN pip install -r requirements.txt

# Clonar el manager de ComfyUI para instalar otros nodos
RUN git clone https://github.com/ltdrdata/ComfyUI-Manager.git custom_nodes/ComfyUI-Manager

# Clonar los nodos personalizados esenciales para nuestro workflow
RUN git clone https://github.com/Fannovel16/comfyui_controlnet_aux.git custom_nodes/comfyui_controlnet_aux
RUN git clone https://github.com/cubiq/ComfyUI_IPAdapter_plus.git custom_nodes/ComfyUI_IPAdapter_plus
RUN git clone https://github.com/Kosinkadink/ComfyUI-AnimateDiff-Evolved.git custom_nodes/ComfyUI-AnimateDiff-Evolved
RUN git clone https://github.com/jags111/ComfyUI-Save-Images-As-Webp.git custom_nodes/ComfyUI-Save-Images-As-Webp
# ... (aquí añadiríamos más nodos si fueran necesarios) ...

# Volver al directorio principal
WORKDIR /

# --- Descarga de Modelos ---
# Esto hace que la imagen sea GRANDE, pero los "cold starts" serán mucho más rápidos.

# 1. Checkpoint Fotorrealista (Ej: Realistic Vision v6.0)
RUN wget -O /ComfyUI/models/checkpoints/realisticVisionV60.safetensors https://civitai.com/api/download/models/130072

# 2. Modelos de ControlNet (OpenPose y SoftEdge)
RUN wget -O /ComfyUI/models/controlnet/control_v11p_sd15_openpose.pth https://huggingface.co/lllyasviel/ControlNet-v1-1/resolve/main/control_v11p_sd15_openpose.pth
RUN wget -O /ComfyUI/models/controlnet/control_v11p_sd15_softedge.pth https://huggingface.co/lllyasviel/ControlNet-v1-1/resolve/main/control_v11p_sd15_softedge.pth

# 3. Modelos de IP-Adapter
RUN wget -O /ComfyUI/models/ipadapter/ip-adapter-plus-face_sd15.bin https://huggingface.co/h94/IP-Adapter/resolve/main/models/ip-adapter-plus-face_sd15.bin
RUN wget -O /ComfyUI/models/clip_vision/CLIP-ViT-H-14-laion2B-s32B-b79K.safetensors https://huggingface.co/h94/IP-Adapter/resolve/main/models/image_encoder/CLIP-ViT-H-14-laion2B-s32B-b79K.safetensors

# 4. Modelos de AnimateDiff
RUN wget -O /ComfyUI/custom_nodes/ComfyUI-AnimateDiff-Evolved/models/mm_sd_v15_v2.ckpt https://huggingface.co/guoyww/animatediff/resolve/main/mm_sd_v15_v2.ckpt


# --- Preparación Final ---
# Copiar nuestro script handler
COPY workflow_api.json .
COPY app_handler.py .

# Exponer el puerto por si queremos probarlo en un Pod estándar (opcional)
EXPOSE 8080

# Comando que se ejecuta cuando el worker serverless se inicia
CMD ["python", "-u", "app_handler.py"]
