# ── Stage 0: deps ────────────────────────────────────────────────────────
# System dependencies for compiling C/Fortran extensions.
FROM python:3.12-slim AS deps

ARG PUID
ARG PGID

RUN groupadd -g ${PGID} mygroup && \
    useradd -u ${PUID} -g mygroup -m myuser

RUN apt-get update && apt-get upgrade --yes && \
    apt-get install --no-install-recommends -y \
    gfortran \
    libgsl-dev \
    libopenblas-dev \
    liblapack-dev \
    build-essential \
    pkg-config \
    libfreetype6-dev \
    libjpeg-dev \
    libpng-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*


# ── Stage 1: build ──────────────────────────────────────────────────────
# Install ceres3 from PyPI (compiles C/Fortran extensions).
FROM deps AS build

RUN pip install --no-cache-dir meson-python meson ninja numpy && \
    pip install --no-cache-dir ceres3 && \
    pip install --no-cache-dir redis python-dotenv


# ── Stage 2: worker ─────────────────────────────────────────────────────
# Thin ExoAutomata worker that imports ceres3.
FROM build AS worker

WORKDIR /app

COPY ./ceres3/worker.py /app/worker.py
COPY ./ceres3/config.py /app/config.py
COPY ./shared /app/shared
COPY ./shared /app/shared

# Symlink COELHO_MODELS from data volume into installed package
RUN mkdir -p /data/ceres_models && \
    SITE=$(python3 -c "import sysconfig; print(sysconfig.get_paths()['purelib'])") && \
    mkdir -p "$SITE/ceres3/data" && \
    ln -sf /data/ceres_models/COELHO_MODELS "$SITE/ceres3/data/COELHO_MODELS"

RUN chown -R myuser:mygroup /app

USER myuser

CMD ["python", "worker.py"]
