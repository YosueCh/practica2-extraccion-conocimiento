import os
import re
import pandas as pd
from sqlalchemy import create_engine

def run_ingestion():
    csv_path = '/opt/airflow/data/ecommerce_data.csv'

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"El dataset no se encuentra en: {csv_path}")

    print("Iniciando la lectura del dataset transaccional...")
    df = pd.read_csv(csv_path, encoding='ISO-8859-1')

    print("Aplicando reglas de limpieza tipográfica en memoria...")

    # REGLA 1: Remover caracteres especiales en descripción
    def clean_special_chars(text):
        if pd.isna(text):
            return "SIN_DESCRIPCION"
        cleaned = re.sub(r'[^a-zA-Z0-9\s\-]', '', str(text))
        return cleaned

    df['Description'] = df['Description'].apply(clean_special_chars)

    # REGLA 2: Normalizar espacios en blanco
    def normalize_spacing(text):
        if pd.isna(text):
            return text
        return re.sub(r'\s+', ' ', str(text)).strip()

    df['Description'] = df['Description'].apply(normalize_spacing)
    if 'CustomerID' in df.columns:
        df['CustomerID'] = df['CustomerID'].astype(str).str.strip()

    # REGLA 3: Validar formato de InvoiceNo
    def validate_invoice_format(invoice):
        inv_str = str(invoice).strip()
        if re.match(r'^C?\d{6}$', inv_str):
            return inv_str
        return "INVALID_INVOICE"

    df['InvoiceNo'] = df['InvoiceNo'].apply(validate_invoice_format)

    # Conexión a PostgreSQL (Alternativa B)
    DB_USER = "uteq_user"
    DB_PASSWORD = "uteq_password"
    DB_HOST = "postgres_staging"
    DB_PORT = "5432"
    DB_DB = "dw_analytics"

    conn_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_DB}"
    engine = create_engine(conn_str)

    print("Cargando información al Staging Area...")
    df.to_sql(
        name='stg_ecommerce_sales',
        con=engine,
        if_exists='replace',
        index=False,
        chunksize=10000,
        method='multi'
    )
    print("¡Ingesta finalizada con éxito!")

if __name__ == "__main__":
    run_ingestion()