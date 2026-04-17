import boto3
import os
from dotenv import load_dotenv

# ================================
# 1. Cargar variables de entorno
# ================================
load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# ================================
# 2. Inicializar cliente S3
# ================================
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

# ================================
# 3. Función de upload
# ================================
def upload_file(local_path: str, s3_key: str):
    """
    Sube un archivo local a S3

    :param local_path: ruta en tu máquina
    :param s3_key: ruta dentro de S3
    """
    try:
        s3_client.upload_file(local_path, BUCKET_NAME, s3_key)
        print(f"✅ Uploaded: {local_path} → s3://{BUCKET_NAME}/{s3_key}")
    except Exception as e:
        print(f"❌ Error uploading {local_path}: {e}")

# ================================
# 4. Ejecución principal
# ================================
if __name__ == "__main__":

    files_to_upload = [
        {
            "local": "data/raw/olist_orders_dataset.csv",
            "s3": "raw/orders/olist_orders_dataset.csv"
        }
    ]

    for file in files_to_upload:
        upload_file(file["local"], file["s3"])