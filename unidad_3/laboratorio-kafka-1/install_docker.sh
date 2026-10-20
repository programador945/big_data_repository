#!/usr/bin/env bash

set -Eeuo pipefail

# Instalación de Docker Engine y Docker Compose
# Compatible con Ubuntu usando el repositorio oficial de Docker.

if [[ "${EUID}" -ne 0 ]]; then
    echo "Ejecuta este script con sudo:"
    echo "sudo bash install_docker.sh"
    exit 1
fi

echo "========================================"
echo " Instalación de Docker para Ubuntu"
echo "========================================"

# Verificar sistema operativo
if [[ ! -f /etc/os-release ]]; then
    echo "No se pudo identificar el sistema operativo."
    exit 1
fi

source /etc/os-release

if [[ "${ID}" != "ubuntu" ]]; then
    echo "Este script está diseñado para Ubuntu."
    echo "Sistema detectado: ${PRETTY_NAME:-desconocido}"
    exit 1
fi

# Actualizar índices e instalar dependencias
echo "[1/6] Actualizando paquetes..."
apt-get update

echo "[2/6] Instalando dependencias..."
apt-get install -y ca-certificates curl

# Configurar clave oficial de Docker
echo "[3/6] Configurando repositorio oficial..."

install -m 0755 -d /etc/apt/keyrings

curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
    -o /etc/apt/keyrings/docker.asc

chmod a+r /etc/apt/keyrings/docker.asc

# Configurar repositorio para la arquitectura y versión de Ubuntu
ARCH="$(dpkg --print-architecture)"
UBUNTU_CODENAME="${UBUNTU_CODENAME:-$VERSION_CODENAME}"

echo \
  "deb [arch=${ARCH} signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${UBUNTU_CODENAME} stable" \
  > /etc/apt/sources.list.d/docker.list

echo "[4/6] Actualizando índices del repositorio Docker..."
apt-get update

# Instalar Docker Engine y complementos
echo "[5/6] Instalando Docker Engine y Docker Compose..."

apt-get install -y \
    docker-ce \
    docker-ce-cli \
    containerd.io \
    docker-buildx-plugin \
    docker-compose-plugin

# Habilitar e iniciar Docker
systemctl enable --now docker

echo "[6/6] Verificando instalación..."

docker --version
docker compose version
systemctl is-active --quiet docker

# Prueba básica del motor
docker run --rm hello-world

echo
echo "========================================"
echo " Instalación completada correctamente"
echo "========================================"
echo "Docker Engine y Docker Compose están listos."
echo "Usa: sudo docker ps"