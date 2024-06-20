# Importa biblitecas para tratativa de dados
from googleapiclient.discovery import build
from google.oauth2 import service_account
import pandas as pd
import socket    

# Define um tempo de timeout para o processo
socket.setdefaulttimeout(300)

# Define a pasta em que o arquivo json de autenticação está
SERVICE_ACCOUNT_FILE = r'C:\Users\U002669\Desktop\pasta_json\autenticacao.json'

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

# Define o id da planilha no google Sheets
sheet_id = '1Q07l7m9jj4CEHtvuBc0tiQOedNSSz28Sed2EAZShlEc'

# Não precisa ler a planilha caso não queira
'''
# Leitura planilha: lê todas as linhas da aba 'Página1'
result = sheet.values().get(spreadsheetId=sheet_id, range='Página1').execute()
# Transforma em dataframe, extraindo a partir da linha 1 da coluna A e definindo a linha 0 como cabeçalho 
base = pd.DataFrame(result['values'][1:], columns=result['values'][0])
'''

# Input valores sheet
lista = [['Coluna_a_linha12', 'Coluna_b_linha12']]

value_range_body = {"values": lista}

# Não precisa apagar os dados caso não queira
''' 
print("Limpando base sheets...", end=' ', flush=True)
sheet.values().clear(spreadsheetId=sheet_id,
                     range="Página1!A:B").execute()
'''

print('Realizando importação via API...', end=' ', flush=True)
sheet.values().append(spreadsheetId=sheet_id,
                      range="Página1!A:B",
                      valueInputOption='USER_ENTERED',
                      insertDataOption='OVERWRITE',
                      body=value_range_body).execute()
