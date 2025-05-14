import os
import yaml
import boto3
from datetime import datetime, timedelta
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from botocore.exceptions import ClientError
import logging
from botocore.config import Config
from botocore.exceptions import EndpointConnectionError
from dotenv import load_dotenv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formataddr

load_dotenv('config/config.ini')

AWS_REGION = os.getenv('AWS_REGION')
AWS_CREDENTIALS_FILE = os.getenv('AWS_CREDENTIALS_FILE')
CONFIG_FILE = os.getenv('CONFIG_FILE')
ALERT_SIZE_CHANGE_SIZE = os.getenv('ALERT_SIZE_CHANGE_SIZE')
SEND_ALERTS_ON_SIZE_CHANGE = os.getenv('SEND_ALERTS_ON_SIZE_CHANGE')
SEND_ALERTS_ON_NO_FILE = os.getenv('SEND_ALERTS_ON_NO_FILE')
SEND_ALERTS_ON_AWS_ERROR = os.getenv('SEND_ALERTS_ON_AWS_ERROR')
SEND_DAILY_STATUS = os.getenv('SEND_DAILY_STATUS')
smtp_server = os.getenv('SMTP_SERVER')
smtp_port = os.getenv('SMTP_PORT')
smtp_user = os.getenv('SMTP_USER')
smtp_password = os.getenv('SMPT_PASSWORD')
alert_from = os.getenv('ALERT_FROM')
alerto_to = os.getenv('ALERT_TO')

# Cargar la configuración desde el archivo YAML
def load_config(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

# Leer el archivo de configuración de credenciales AWS en formato credentials
def load_aws_credentials(file_path):
    with open(file_path, 'r') as file:
        credentials = {}
        current_profile = None
        for line in file:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if line.startswith('[') and line.endswith(']'):  # Detectar un nuevo perfil
                current_profile = line[1:-1].strip()
                credentials[current_profile] = {}
            elif '=' in line and current_profile:
                key, value = line.split('=', 1)
                credentials[current_profile][key.strip()] = value.strip()
        return credentials

# Inicializar el cliente de S3 usando un perfil específico
def initialize_s3_client_with_profile(profile_name):
    try:
        # Leer las credenciales del perfil
        credentials = AWS_CREDENTIALS.get(profile_name, {})
        aws_access_key_id = credentials.get('aws_access_key_id')
        aws_secret_access_key = credentials.get('aws_secret_access_key')

        if not aws_access_key_id or not aws_secret_access_key:
            if SEND_ALERTS_ON_AWS_ERROR == 'True':
                subject = f"Error de credenciales para el perfil {profile_name}"
                body = f"Credenciales no encontradas o incompletas para el perfil: {profile_name}"
                send_mail(subject, body)
            raise ValueError(f"Credenciales no encontradas o incompletas para el perfil: {profile_name}")

        # Crear el cliente de S3
        s3_client = boto3.client(
            's3',
            region_name=AWS_REGION,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            config=Config(connect_timeout=10, read_timeout=30)  # Tiempo de espera en segundos
        )
        return s3_client
    except EndpointConnectionError as e:
        if SEND_ALERTS_ON_AWS_ERROR == 'True':
            subject = f"Error de conexión al endpoint de AWS para el perfil {profile_name}"
            body = f"No se pudo conectar al endpoint de AWS para el perfil: {profile_name}. Error: {e}"
            send_mail(subject, body)
        print(f"ERROR: No se pudo conectar al endpoint de AWS para el perfil {profile_name}: {e}")
        raise
    except Exception as e:
        if SEND_ALERTS_ON_AWS_ERROR == 'True':
            subject = f"Error al inicializar el cliente S3 para el perfil {profile_name}"
            body = f"Error al inicializar el cliente S3 para el perfil: {profile_name}. Error: {e}"
            send_mail(subject, body)
        print(f"ERROR: Error al inicializar el cliente S3 para el perfil {profile_name}: {e}")
        raise

def check_repositories_for_today(config):
    try:
        print("Iniciando la verificación de repositorios...")

        grouped_repositories = {}
        for repo in config['repositories']:
            profile_name = repo['profile']
            if profile_name not in grouped_repositories:
                grouped_repositories[profile_name] = []
            grouped_repositories[profile_name].append(repo)

        repo_details = []  # Para el detalle de cada repo

        for profile_name, repositories in grouped_repositories.items():
            print(f"\nProcesando grupo de repositorios con el perfil: {profile_name}\n")
            print(f"Repositorios en este grupo: {[repo['name'] for repo in repositories]}\n")

            try:
                print(f"Inicializando cliente S3 para el perfil: {profile_name}")
                s3_client = initialize_s3_client_with_profile(profile_name)
                print("Cliente S3 inicializado correctamente.\n")
            except Exception as e:
                if SEND_ALERTS_ON_AWS_ERROR == 'True':
                    subject = f"Error al inicializar el cliente S3 para el perfil {profile_name}"
                    body = f"Error al inicializar el cliente S3 para el perfil: {profile_name}. Error: {e}"
                    send_mail(subject, body)
                print(f"FAIL: Error al inicializar cliente S3 para el perfil {profile_name}: {e}\n")
                continue

            success_repos = []
            success_repos_compare = []
            failure_repos = []
            failure_repos_compare = []

            for repo in repositories:
                bucket_url = repo['bucket']
                bucket_parts = bucket_url.replace("s3://", "").split("/", 1)
                bucket_name = bucket_parts[0]
                prefix = bucket_parts[1] if len(bucket_parts) > 1 else ""

                try:
                    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
                    detail = f"\nRepositorio: {repo['name']} ({repo.get('tagg', 'No Tag')})\n"
                    if 'Contents' in response:
                        today = datetime.now().date()
                        has_today_file = any(
                            obj['LastModified'].date() == today for obj in response.get('Contents', [])
                        )

                        if has_today_file:
                            print(f"{repo['name']} ({repo.get('tagg', 'No Tag')}) Se realizó OK el día de hoy.")
                            success_repos.append(f"{repo['name']} ({repo.get('tagg', 'No Tag')})")
                            detail += "Estado: OK - Se realizó backup hoy.\n"
                            archivos = sorted(response['Contents'], key=lambda obj: obj['LastModified'], reverse=True)
                            for obj in archivos:
                                size_human = human_readable_size(obj['Size'])
                                fecha = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                                detail += f"  - {obj['Key']} | {size_human} | {fecha}\n"
                            compare = compare_file_sizes(s3_client, bucket_name, prefix)
                            if compare is True:
                                detail += "  * Cambio significativo en el tamaño de los archivos.\n"
                                failure_repos_compare.append(f"{repo['name']} ({repo.get('tagg', 'No Tag')})")
                            elif compare is False:
                                detail += "  * No se detectó un cambio significativo en el tamaño de los archivos.\n"
                                success_repos_compare.append(f"{repo['name']} ({repo.get('tagg', 'No Tag')})")
                            print("\n")
                        else:
                            failure_repos.append(f"{repo['name']} ({repo.get('tagg', 'No Tag')})")
                            detail += "Estado: FAIL - No se encontró archivo con fecha de hoy.\n"
                            print(f"{repo['name']} ({repo.get('tagg', 'No Tag')}) - No se encontró archivo con fecha de hoy.\n")
                    else:
                        failure_repos.append(f"{repo['name']} ({repo.get('tagg', 'No Tag')})")
                        detail += "Estado: FAIL - No se encontraron objetos.\n"
                        print(f"{repo['name']} ({repo.get('tagg', 'No Tag')}) - No se encontraron objetos.")
                    repo_details.append(detail)
                except ClientError as e:
                    if e.response['Error']['Code'] == 'NoSuchBucket':
                        print(f"FAIL: El bucket {bucket_name} no existe.")
                        failure_repos.append(f"{repo['name']} ({repo.get('tagg', 'No Tag')})")
                        repo_details.append(f"\nRepositorio: {repo['name']} ({repo.get('tagg', 'No Tag')})\nEstado: FAIL - El bucket no existe.\n")
                    elif e.response['Error']['Code'] == 'InvalidBucketName':
                        print(f"FAIL: Nombre de bucket inválido: {bucket_name}")
                        failure_repos.append(f"{repo['name']} ({repo.get('tagg', 'No Tag')})")
                        repo_details.append(f"\nRepositorio: {repo['name']} ({repo.get('tagg', 'No Tag')})\nEstado: FAIL - Nombre de bucket inválido.\n")
                    else:
                        print(f"FAIL: Error al listar objetos en el bucket {bucket_name}: {e}")
                        failure_repos.append(f"{repo['name']} ({repo.get('tagg', 'No Tag')})")
                        repo_details.append(f"\nRepositorio: {repo['name']} ({repo.get('tagg', 'No Tag')})\nEstado: FAIL - Error al listar objetos: {e}\n")
                    print(f"FAIL: {repo['name']} ({repo.get('tagg', 'No Tag')}) - Error al listar objetos: {e}")

            print("\nResumen de la verificación de repositorios:")
            print(f"Repositorios procesados: {len(repositories)}")
            print(f"Repositorios exitosos: {len(success_repos)}")
            print(f"Lista de repositorios exitosos:\n" + "\n".join(f"- {repo}" for repo in success_repos))
            print(f"Lista de repositorios fallidos:\n" + "\n".join(f"- {repo}" for repo in failure_repos))
            print(f"Repositorios fallidos: {len(failure_repos)}")
            print(f"Repositorios con cambio de tamaño: {len(failure_repos_compare)}")
            print(f"Repositorios sin cambio de tamaño: {len(success_repos_compare)}")

            if len(failure_repos) > 0 and SEND_ALERTS_ON_NO_FILE == 'True':
                subject = "BACKUP FALLIDO - No se encontró archivo " + datetime.now().strftime("%Y-%m-%d")
                body = f"Se encontraron errores al verificar los siguientes repositorios:\n" + "\n".join(f"- {repo}" for repo in failure_repos)
                send_mail(subject, body)
            if len(failure_repos_compare) > 0 and SEND_ALERTS_ON_SIZE_CHANGE == 'True':
                subject = "BACKUP FALLIDO - Tamaño " + datetime.now().strftime("%Y-%m-%d")
                body = f"Se encontraron cambios significativos en el tamaño de los siguientes repositorios:\n" + "\n".join(f"- {repo}" for repo in failure_repos_compare)
                send_mail(subject, body)
            if len(success_repos) > 0 and SEND_DAILY_STATUS == 'True':
                subject = "Backup Daily status de: " + datetime.now().strftime("%Y-%m-%d")
                body = (
                    "Resumen de la verificación de repositorios:\n"
                    "Backup exitosos:\n" +
                    "\n".join(f"- {repo}" for repo in success_repos) +
                    "\n\nBackup fallidos:\n" +
                    "\n".join(f"- {repo}" for repo in failure_repos) +
                    "\n"
                    "\n"
                    f"Repositorios procesados: {len(repositories)}\n"
                    f"Repositorios exitosos: {len(success_repos)}\n"
                    f"Repositorios fallidos: {len(failure_repos)}\n"
                    f"Repositorios con cambio de tamaño: {len(failure_repos_compare)}\n"
                    f"Repositorios sin cambio de tamaño: {len(success_repos_compare)}\n"
                    "\nDETALLE POR REPOSITORIO:\n"
                    + "\n".join(repo_details)
                )
                send_mail(subject, body)
    except Exception as e:
        if SEND_ALERTS_ON_AWS_ERROR == 'True':
            subject = "Error inesperado al verificar los repositorios"
            body = f"Error inesperado al verificar los repositorios: {e}"
            send_mail(subject, body)
        print(f"ERROR: Error inesperado al verificar los repositorios: {e}")
        raise

def compare_file_sizes(s3_client, bucket_name, prefix):
    try:
        print(f"Comparando tamaños de archivos en el bucket: {bucket_name} con prefijo: {prefix}")

        # Listar objetos en el bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' not in response or len(response['Contents']) < 2:
            print(f"No hay suficientes archivos en el bucket {bucket_name} con prefijo {prefix} para comparar.")
            return

        # Ordenar los objetos por fecha de modificación (de más reciente a más antiguo)
        sorted_objects = sorted(response['Contents'], key=lambda obj: obj['LastModified'], reverse=True)

        # Obtener el último y el anteúltimo archivo
        latest_file = sorted_objects[0]
        second_latest_file = sorted_objects[1]

        # Obtener los tamaños de los archivos
        latest_size = latest_file['Size']
        second_latest_size = second_latest_file['Size']

        percentage_change = ((latest_size - second_latest_size) / second_latest_size) * 100

        # Convertir los tamaños a un formato legible
        latest_size_human = human_readable_size(latest_size)
        second_latest_size_human = human_readable_size(second_latest_size)

        # Imprimir el resultado
        print(f"Último archivo: {latest_file['Key']} (Tamaño: {latest_size_human})")
        print(f"Anteúltimo archivo: {second_latest_file['Key']} (Tamaño: {second_latest_size_human})")
        if percentage_change > float(ALERT_SIZE_CHANGE_SIZE):
            return True
        else:
            return False
    except ClientError as e:
        print(f"FAIL: Error al listar objetos en el bucket {bucket_name}: {e}")
    except Exception as e:
        print(f"ERROR: Error inesperado al comparar tamaños de archivos: {e}")

def human_readable_size(size_in_bytes):
    """Convierte un tamaño en bytes a un formato legible (KB, MB, GB, etc.)."""
    for unit in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if size_in_bytes < 1024:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024

def send_mail(subject, body):

    message = MIMEMultipart()
    message["From"] = formataddr((alert_from, smtp_user))
    message["To"] = alerto_to
    message["Subject"] = subject
    message.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as servidor:
            servidor.starttls()
            servidor.login(smtp_user, smtp_password)
            servidor.send_message(message)
            print("Correo enviado exitosamente.")
    except Exception as e:
        print(f"Error al enviar el correo: {e}")

CONFIG = load_config(CONFIG_FILE)
AWS_CREDENTIALS = load_aws_credentials(AWS_CREDENTIALS_FILE)

check_repositories_for_today(CONFIG)

