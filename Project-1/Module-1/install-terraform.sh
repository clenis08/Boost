#!/bin/bash

# Asegurarse de que el script se ejecute con permisos de superusuario
if [ "$(id -u)" -ne "0" ]; then
    echo "Este script debe ejecutarse como root o usando sudo." 1>&2
    exit 1
fi

# Actualizar el sistema e instalar paquetes necesarios
apt-get update && apt-get install -y gnupg software-properties-common curl

# Instalar la clave GPG de HashiCorp
wget -O- https://apt.releases.hashicorp.com/gpg | gpg --dearmor | tee /usr/share/keyrings/hashicorp-archive-keyring.gpg > /dev/null

# Verificar la huella digital de la clave GPG
gpg --no-default-keyring --keyring /usr/share/keyrings/hashicorp-archive-keyring.gpg --fingerprint

# Agregar el repositorio oficial de HashiCorp
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/hashicorp.list

# Actualizar la información del paquete e instalar Terraform
apt update && apt-get install -y terraform

echo "Terraform ha sido instalado exitosamente."

