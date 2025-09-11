import os
import requests
import pandas as pd
from datetime import datetime
from databricks import sql

def ingest_api_users():
    """
    Ingere dados de usuários da API JSONPlaceholder e carrega no Databricks.
    """
    print("Iniciando ingestão de dados de usuários...")

    # 1. EXTRAÇÃO (API pública, sem autenticação)
    url = "https://jsonplaceholder.typicode.com/users"
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"Erro ao acessar a API: Status {response.status_code}")
        print(response.text)
        return

    data = response.json()
    print(f"{len(data)} usuários extraídos com sucesso.")

    # 2. TRANSFORMAÇÃO SIMPLES
    users = []
    for user in data:
        users.append({
            'id': user.get('id'),
            'name': user.get('name'),
            'username': user.get('username'),
            'email': user.get('email'),
            'phone': user.get('phone'),
            'website': user.get('website'),
            'city': user['address'].get('city') if user.get('address') else None,
            'company_name': user['company'].get('name') if user.get('company') else None,
            '_ingested_at': datetime.now()
        })
    df = pd.DataFrame(users)
    print("Dados transformados em um DataFrame.")

    # 3. CARREGAMENTO (Load para o Databricks)
    databricks_host = os.getenv("DATABRICKS_HOST")
    databricks_token = os.getenv("DATABRICKS_TOKEN")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")

    if not all([databricks_host, databricks_token, http_path]):
        raise ValueError("As credenciais do Databricks não foram encontradas nas variáveis de ambiente.")

    print("Conectando ao Databricks e carregando os dados...")
    with sql.connect(server_hostname=databricks_host,
                     http_path=http_path,
                     access_token=databricks_token) as connection:
        with connection.cursor() as cursor:
            # Assume que a tabela já existe
            # O ideal é usar MERGE, mas INSERT é mais simples para começar
            data_to_insert = [tuple(row) for row in df.values.tolist()]
            cursor.execute("USE raw.jsonplaceholder;")
            cursor.executemany(
                "INSERT INTO api_users VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                data_to_insert
            )
            print(f"{len(data_to_insert)} linhas inseridas no Databricks com sucesso.")

if __name__ == "__main__":
    ingest_api_users()
