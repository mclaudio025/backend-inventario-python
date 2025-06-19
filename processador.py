import pandas as pd
import requests
import time
import io  # Essencial para ler o arquivo em memória
from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- CONFIGURAÇÕES ---
# Certifique-se que este é o nome do seu arquivo JSON de credenciais
SERVICE_ACCOUNT_FILE = 'n8n-integracao-451311-7ef61a9b23e7.json'
# O ID da pasta no Google Drive onde estão as planilhas
FOLDER_ID = '1e8A6j9r27Ydu37WqtKaKjN1bTW2tDqGw' # Este ID veio do seu fluxo n8n
# A URL do seu servidor FastAPI que está rodando localmente
API_URL = "http://127.0.0.1:8000/produtos/sincronizar"
# Escopos de permissão (apenas leitura do Drive é necessária)
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
# -------------------

def buscar_e_processar_planilha():
    """
    Função principal que conecta ao Google Drive, baixa a planilha mais recente
    e envia os dados para a API de sincronização.
    """
    try:
        # 1. AUTENTICAÇÃO COM A API DO GOOGLE
        print("Autenticando com a API do Google...")
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build('drive', 'v3', credentials=creds)
        print("Autenticação bem-sucedida.")

        # 2. BUSCA DO ARQUIVO NA PASTA
        print(f"Buscando planilha na pasta do Google Drive...")
        # Busca por planilhas (.xlsx) e ordena pela data de modificação para pegar a mais recente
        query = f"'{FOLDER_ID}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
        results = service.files().list(
            q=query,
            pageSize=1,
            fields="files(id, name)",
            orderBy="modifiedTime desc"
        ).execute()
        items = results.get('files', [])

        if not items:
            print("ERRO: Nenhuma planilha encontrada na pasta do Google Drive.")
            return

        # 3. DOWNLOAD DO ARQUIVO
        latest_file = items[0]
        print(f"Arquivo encontrado: '{latest_file['name']}'. Iniciando download...")
        request = service.files().get_media(fileId=latest_file['id'])
        # Usamos io.BytesIO para tratar o arquivo baixado em memória, sem precisar salvar no disco
        file_content_in_memory = io.BytesIO(request.execute())
        print("Download concluído.")

        # 4. LEITURA E PROCESSAMENTO COM PANDAS
        print("Lendo e processando a planilha...")
        df = pd.read_excel(file_content_in_memory)
        print(f"Planilha lida. Total de {len(df)} produtos para sincronizar.")

        # 5. LOOP DE SINCRONIZAÇÃO
        for index, row in df.iterrows():
            try:
                # Monta o payload com toda a limpeza de dados que aprendemos
                payload = {
                    "estado": str(row['estado']),
                    "codigo": str(row['código']).strip().zfill(6),
                    "cod_barra": str(row['cód.barra']).strip() if pd.notna(row['cód.barra']) else None,
                    "descricao": str(row['descrição']),
                    "sloja": int(row['sloja']) if pd.notna(row['sloja']) else 0,
                    "sestoque": int(row['sestoque']) if pd.notna(row['sestoque']) else 0,
                    "preco": float(row['preço']),
                    "sminimo": int(row['sminimo']) if pd.notna(row['sminimo']) else 0,
                    "smaximo": int(row['smaximo']) if pd.notna(row['smaximo']) else 0,
                    "loja": str(row['loja'])
                }

                # Envia para a nossa API
                response = requests.post(API_URL, json=payload)
                if response.status_code == 200:
                    print(f"  - Linha {index + 2}: Produto {payload['codigo']} sincronizado -> {response.json().get('operacao')}")
                else:
                    print(f"  - Linha {index + 2}: Falha -> Status {response.status_code}, Resposta: {response.text}")

            except KeyError as e:
                print(f"  - ERRO de processamento na linha {index + 2}: A coluna {e} não foi encontrada. Pulando linha.")
            except Exception as e:
                print(f"  - ERRO inesperado na linha {index + 2}: {e}. Pulando linha.")

        print("\nSincronização concluída!")

    except Exception as e:
        print(f"\nOcorreu um erro geral no processo: {e}")

# Ponto de entrada do nosso script
if __name__ == "__main__":
    buscar_e_processar_planilha()