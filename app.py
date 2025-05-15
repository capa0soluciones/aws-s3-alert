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

def load_config(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

def load_aws_credentials(file_path):
    with open(file_path, 'r') as file:
        credentials = {}
        current_profile = None
        for line in file:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if line.startswith('[') and line.endswith(']'):
                current_profile = line[1:-1].strip()
                credentials[current_profile] = {}
            elif '=' in line and current_profile:
                key, value = line.split('=', 1)
                credentials[current_profile][key.strip()] = value.strip()
        return credentials

def initialize_s3_client_with_profile(profile_name):
    try:
        credentials = AWS_CREDENTIALS.get(profile_name, {})
        aws_access_key_id = credentials.get('aws_access_key_id')
        aws_secret_access_key = credentials.get('aws_secret_access_key')

        if not aws_access_key_id or not aws_secret_access_key:
            if SEND_ALERTS_ON_AWS_ERROR == 'True':
                subject = f"Error de credenciales para el perfil {profile_name}"
                body = f"Credenciales no encontradas o incompletas para el perfil: {profile_name}"
                send_mail(subject, body)
            raise ValueError(f"Credenciales no encontradas o incompletas para el perfil: {profile_name}")

        s3_client = boto3.client(
            's3',
            region_name=AWS_REGION,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            config=Config(connect_timeout=10, read_timeout=30)
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

def compare_file_sizes(s3_client, bucket_name, prefix):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' not in response or len(response['Contents']) < 2:
            return None

        sorted_objects = sorted(response['Contents'], key=lambda obj: obj['LastModified'], reverse=True)
        latest_file = sorted_objects[0]
        second_latest_file = sorted_objects[1]
        latest_size = latest_file['Size']
        second_latest_size = second_latest_file['Size']

        percentage_change = ((latest_size - second_latest_size) / second_latest_size) * 100

        if percentage_change > float(ALERT_SIZE_CHANGE_SIZE):
            return True
        else:
            return False
    except Exception:
        return None

def human_readable_size(size_in_bytes):
    for unit in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if size_in_bytes < 1024:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024

def send_mail(subject, body, html=False):
    message = MIMEMultipart()
    message["From"] = formataddr((alert_from, smtp_user))
    message["To"] = alerto_to
    message["Subject"] = subject
    if html:
        message.attach(MIMEText(body, "html"))
    else:
        message.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(smtp_server, int(smtp_port)) as servidor:
            servidor.starttls()
            servidor.login(smtp_user, smtp_password)
            servidor.send_message(message)
            print("Correo enviado exitosamente.")
    except Exception as e:
        print(f"Error al enviar el correo: {e}")

def check_repositories_for_today(config):
    try:
        print("Iniciando la verificación de repositorios...")

        grouped_repositories = {}
        for repo in config['repositories']:
            profile_name = repo['profile']
            if profile_name not in grouped_repositories:
                grouped_repositories[profile_name] = []
            grouped_repositories[profile_name].append(repo)

        # Acumuladores globales para el resumen final
        all_success_repos = []
        all_success_repos_compare = []
        all_failure_repos = []
        all_failure_repos_compare = []
        all_repo_details_html = []
        total_repos = 0

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

            for repo in repositories:
                total_repos += 1
                bucket_url = repo['bucket']
                bucket_parts = bucket_url.replace("s3://", "").split("/", 1)
                bucket_name = bucket_parts[0]
                prefix = bucket_parts[1] if len(bucket_parts) > 1 else ""

                try:
                    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
                    repo_name = repo['name']
                    repo_tagg = repo.get('tagg', 'No Tag')
                    detail_html = f"<div style='margin-bottom:18px;'><b>{repo_name}</b> <span style='color:#888;'>({repo_tagg})</span><br>"

                    if 'Contents' in response:
                        today = datetime.now().date()
                        has_today_file = any(
                            obj['LastModified'].date() == today for obj in response.get('Contents', [])
                        )

                        archivos = sorted(response['Contents'], key=lambda obj: obj['LastModified'], reverse=True)[:10]
                        if has_today_file:
                            all_success_repos.append(f"{repo_name} ({repo_tagg})")
                            detail_html += "<span style='color:green;'>✅ Estado: OK - Se realizó backup hoy.</span><br>"
                            detail_html += "<ul style='margin:6px 0 6px 0;'>"
                            for obj in archivos:
                                size_human = human_readable_size(obj['Size'])
                                fecha = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                                detail_html += f"<li>{obj['Key']} | {size_human} | {fecha}</li>"
                            detail_html += "</ul>"
                            compare = compare_file_sizes(s3_client, bucket_name, prefix)
                            if compare is True:
                                detail_html += "<span style='color:orange;'>⚠️ Cambio significativo en el tamaño de los archivos.</span><br>"
                                all_failure_repos_compare.append(f"{repo_name} ({repo_tagg})")
                            elif compare is False:
                                detail_html += "<span style='color:green;'>✔ No se detectó un cambio significativo en el tamaño de los archivos.</span><br>"
                                all_success_repos_compare.append(f"{repo_name} ({repo_tagg})")
                        else:
                            all_failure_repos.append(f"{repo_name} ({repo_tagg})")
                            detail_html += "<span style='color:red;'>❌ Estado: FAIL - No se encontró archivo con fecha de hoy.</span><br>"
                            detail_html += "<ul style='margin:6px 0 6px 0;'>"
                            for obj in archivos:
                                size_human = human_readable_size(obj['Size'])
                                fecha = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                                detail_html += f"<li>{obj['Key']} | {size_human} | {fecha}</li>"
                            detail_html += "</ul>"
                    else:
                        all_failure_repos.append(f"{repo_name} ({repo_tagg})")
                        detail_html += "<span style='color:red;'>❌ Estado: FAIL - No se encontraron objetos.</span><br>"
                    detail_html += "</div>"
                    all_repo_details_html.append(detail_html)
                except ClientError as e:
                    repo_name = repo['name']
                    repo_tagg = repo.get('tagg', 'No Tag')
                    if e.response['Error']['Code'] == 'NoSuchBucket':
                        all_failure_repos.append(f"{repo_name} ({repo_tagg})")
                        all_repo_details_html.append(
                            f"<div style='margin-bottom:18px;'><b>{repo_name}</b> <span style='color:#888;'>({repo_tagg})</span><br>"
                            "<span style='color:red;'>❌ Estado: FAIL - El bucket no existe.</span></div>"
                        )
                    elif e.response['Error']['Code'] == 'InvalidBucketName':
                        all_failure_repos.append(f"{repo_name} ({repo_tagg})")
                        all_repo_details_html.append(
                            f"<div style='margin-bottom:18px;'><b>{repo_name}</b> <span style='color:#888;'>({repo_tagg})</span><br>"
                            "<span style='color:red;'>❌ Estado: FAIL - Nombre de bucket inválido.</span></div>"
                        )
                    else:
                        all_failure_repos.append(f"{repo_name} ({repo_tagg})")
                        all_repo_details_html.append(
                            f"<div style='margin-bottom:18px;'><b>{repo_name}</b> <span style='color:#888;'>({repo_tagg})</span><br>"
                            f"<span style='color:red;'>❌ Estado: FAIL - Error al listar objetos: {e}</span></div>"
                        )

        # ENVÍO DE ALERTAS INDIVIDUALES (opcional, puedes mantenerlo si quieres alertas inmediatas)
        if len(all_failure_repos) > 0 and SEND_ALERTS_ON_NO_FILE == 'True':
            subject = "BACKUP FALLIDO - No se encontró archivo " + datetime.now().strftime("%Y-%m-%d")
            body = "Se encontraron errores al verificar los siguientes repositorios:<br>" + "<br>".join(f"- {repo}" for repo in all_failure_repos)
            send_mail(subject, body, html=True)
        if len(all_failure_repos_compare) > 0 and SEND_ALERTS_ON_SIZE_CHANGE == 'True':
            subject = "BACKUP FALLIDO - Cambio significativo en el tamaño de los archivos " + datetime.now().strftime("%Y-%m-%d")
            body = "Se encontraron cambios significativos en el tamaño de los siguientes repositorios:<br>" + "<br>".join(f"- {repo}" for repo in all_failure_repos_compare)
            send_mail(subject, body, html=True)

        # ENVÍO DEL STATUS DIARIO (UN SOLO MAIL)
        if SEND_DAILY_STATUS == 'True':
            subject = "Backup Daily status - Resumen de la verificación de repositorios " + datetime.now().strftime("%Y-%m-%d")
            body = f"""
            <html>
            <body style="font-family: Arial, sans-serif;">
                <h2>Resumen de la verificación de repositorios</h2>
                <p><b>Repositorios procesados:</b> {total_repos}</p>
                <p><b style='color:green;'>✅ Backups exitosos ({len(all_success_repos)}):</b><br>
                {"<br>".join(f"<span style='color:green;'>✅ {repo}</span>" for repo in all_success_repos) or "<span style='color:gray;'>Ninguno</span>"}
                </p>
                <p><b style='color:red;'>❌ Backups fallidos ({len(all_failure_repos)}):</b><br>
                {"<br>".join(f"<span style='color:red;'>❌ {repo}</span>" for repo in all_failure_repos) or "<span style='color:gray;'>Ninguno</span>"}
                </p>
                <p><b style='color:orange;'>⚠️ Backups con cambio de tamaño ({len(all_failure_repos_compare)}):</b><br>
                {"<br>".join(f"<span style='color:orange;'>⚠️ {repo}</span>" for repo in all_failure_repos_compare) or "<span style='color:gray;'>Ninguno</span>"}
                </p>
                <p><b>Repositorios sin cambio de tamaño:</b> {len(all_success_repos_compare)}</p>
                <hr>
                <h3>Detalle por repositorio (últimos 10 archivos)</h3>
                {''.join(all_repo_details_html)}
            </body>
            </html>
            """
            send_mail(subject, body, html=True)
    except Exception as e:
        if SEND_ALERTS_ON_AWS_ERROR == 'True':
            subject = "Error inesperado al verificar los repositorios"
            body = f"Error inesperado al verificar los repositorios: {e}"
            send_mail(subject, body)
        print(f"ERROR: Error inesperado al verificar los repositorios: {e}")
        raise

CONFIG = load_config(CONFIG_FILE)
AWS_CREDENTIALS = load_aws_credentials(AWS_CREDENTIALS_FILE)

check_repositories_for_today(CONFIG)
