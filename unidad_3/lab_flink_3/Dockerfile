
FROM python:3.11-slim-bookworm

# Instalar Java y herramientas necesarias
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        procps \
        curl \
        && rm -rf /var/lib/apt/lists/*

# Configurar Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Instalar JupyterLab y PyFlink
RUN pip install --no-cache-dir \
    jupyterlab \
    apache-flink==1.20.1

# Directorio de trabajo
WORKDIR /home/jovyan/work

# Puerto de Jupyter
EXPOSE 8888

# Iniciar JupyterLab
CMD ["jupyter", "lab", \
     "--ip=0.0.0.0", \
     "--port=8888", \
     "--no-browser", \
     "--allow-root", \
     "--ServerApp.token=bigdata2026", \
     "--ServerApp.password=", \
     "--ServerApp.root_dir=/home/jovyan/work"]
