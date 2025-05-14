# aws-s3-alert

Este proyecto es un script en Python que verifica diariamente el estado de los backups en buckets de AWS S3, enviando alertas por correo electrónico si detecta problemas como archivos faltantes, cambios significativos de tamaño o errores de acceso a AWS.

## ¿Qué hace este script?

- Lee una lista de repositorios configurados en un archivo YAML.
- Para cada repositorio, verifica si existe un archivo subido en el día actual.
- Compara el tamaño de los dos archivos más recientes para detectar cambios significativos.
- Envía alertas por correo electrónico en caso de:
  - No encontrar archivos del día.
  - Detectar cambios de tamaño superiores al umbral configurado.
  - Errores de acceso a AWS.
- Envía un resumen diario del estado de los backups.

## Estructura del proyecto

```
app.py
docker-compose.yml
Dockerfile
requirements.txt
config/
  config.ini
  credentials
  repositories.yml
```

## Configuración

### 1. Variables de entorno

Las variables de entorno se cargan desde `config/config.ini`. Debes completar este archivo con los valores adecuados:

```ini
AWS_REGION=us-east-1
CONFIG_FILE=config/repositories.yml
AWS_CREDENTIALS_FILE=config/credentials
ALERT_SIZE_CHANGE_SIZE=30
SEND_DAILY_STATUS=True
SEND_ALERTS_ON_SIZE_CHANGE=True
SEND_ALERTS_ON_NO_FILE=True
SEND_ALERTS_ON_AWS_ERROR=True
SMTP_SERVER=smtp.tu-servidor.com
SMTP_PORT=587
SMTP_USER=usuario@dominio.com
SMPT_PASSWORD=tu_password
ALERT_TO=destinatario@dominio.com
ALERT_FROM=Backup Alert
```

> **Nota:** Cambia los valores según tu entorno y necesidades.

### 2. Configuración de credenciales AWS

Edita el archivo [`config/credentials`](config/credentials) con tus credenciales de AWS. Ejemplo:

```
[default]
aws_access_key_id = TU_ACCESS_KEY
aws_secret_access_key = TU_SECRET_KEY
```

Puedes agregar más perfiles si lo necesitas:

```
[otro_perfil]
aws_access_key_id = OTRA_ACCESS_KEY
aws_secret_access_key = OTRA_SECRET_KEY
```

### 3. Configuración de repositorios

Edita [`config/repositories.yml`](config/repositories.yml) para listar los buckets y prefijos a monitorear:

```yaml
repositories:
  - name: "Backup Base de Datos"
    bucket: "s3://mi-bucket/backups"
    profile: "default"
    tagg: "DB"
```

Puedes agregar tantos repositorios como necesites.

## Instalación y uso

### Opción 1: Docker

1. Construye la imagen:

   ```sh
   docker-compose build
   ```

2. Ejecuta el script:

   ```sh
   docker-compose up
   ```

### Opción 2: Local (sin Docker)

1. Instala las dependencias:

   ```sh
   pip install -r requirements.txt
   ```

2. Ejecuta el script:

   ```sh
   python app.py
   ```

## Notas

- El script envía correos usando SMTP. Asegúrate de que los datos del servidor SMTP sean correctos y que el usuario tenga permisos para enviar correos.
- El umbral de alerta por cambio de tamaño (`ALERT_SIZE_CHANGE_SIZE`) es un porcentaje. Por ejemplo, 30 significa que si el archivo de hoy cambia más de un 30% respecto al anterior, se enviará una alerta.
- Puedes agregar/quitar repositorios y perfiles de AWS según tus necesidades.

## Licencia

MIT