# Importa biblitecas para tratativa de dados
from googleapiclient.discovery import build
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pandas as pd
import os
import socket 
import csv

# Define a pasta onde os arquivos CSV estão localizados
pasta = r'G:\Drives compartilhados\arquitetura\Documentos\Arquitetura Operacional\BACKOFFICE FINANCEIRO\DASHBOARD\DASH_OPERACIONAL\LOOKER\Arquivos_diarios\PROCESSAMENTO_DOCK\Parcelado'

# Cria uma Lista vazia para armazenar todos os DataFrames dos arquivos CSV
dfs = []

# Cria a variavel data como vazio
data = ''

# Percorre todos os arquivos na pasta, um por vez
for arquivo in os.listdir(pasta):
    # Se o arquivo terminar com '.csv' ou seja, for do tipo csv
    if arquivo.endswith('.csv'):
        # Define o caminho completo do arquivo como uma junção entre o nome da pasta e do arquivo
        caminho_arquivo = os.path.join(pasta,arquivo)
        # Lê o arquivo CSV
        df_temp = pd.read_csv(caminho_arquivo)
        # Redefine a variavel 'data' como as primeiras 8 letras do nome do arquivo
        data = arquivo[:8]
        # Redefine a varivael data com o formato de data correto
        data = pd.to_datetime(data, format='%d%m%Y')
        # Cria a coluna "DataCompra" com o resultado da variavel data
        df_temp['DATA'] = data
        # Adiciona o DataFrame à lista
        dfs.append(df_temp)

# Junta todos os DataFrames em um único DataFrame
df = pd.concat(dfs, ignore_index=True)

# Retira o "R$" do valor, e faz as modificações necessárias para transforma-los em número
df['Total'] = df['Total'].str.replace('R$ ','')
df['Total'] = df['Total'].str.replace('.','')
df['Total'] = df['Total'].str.replace(',','.')
df['Total'] = df['Total'].astype(float)

# Cria a coluna "DebCre" com o resultado das validações acima
df['DEBCRED'] = 'Credito'

# Cria as coluna "Classificação1" com o texto fixo 'Nacional'
df['CLASSIFICAÇÃO1'] = 'Nacional'

# Cria as coluna "Classificação2" com o texto fixo 'Compras parceladas'
df['CLASSIFICAÇÃO2'] = 'Compras parceladas'

# Cria a coluna "Fonte" com o texto fixo 'Cartão de crédito'
df['FONTE'] = 'Cartão de Crédito'

# Agrupa os valores pelos campos definidos
agrupamento_tipo = df.groupby(['DATA','DEBCRED','CLASSIFICAÇÃO1','CLASSIFICAÇÃO2','FONTE']).agg(soma=('Total','sum'),contagem=('Total','size')).reset_index().round(2)

# Para cada coluna do agrupamento acima
for coluna in agrupamento_tipo.columns:
    # Transforma a coluna em str(texto)
    agrupamento_tipo[coluna] = agrupamento_tipo[coluna].astype(str)

# Transforma o agrupamento em uma lista e o armazena na variavel 'lista'
lista = agrupamento_tipo.values.tolist()

# Define um tempo de timeout para o processo de integração
socket.setdefaulttimeout(300)

# Define a pasta em que o arquivo json de autenticação está
SERVICE_ACCOUNT_FILE = r'C:\Users\U002669\Documents\GitHub\painel_gerencial_backoffice\auten_integ_python_sheets\autenticacao_integracao.json'

# Realiza a conexão com a planilha
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

# Define o id da planilha no google Sheets
sheet_id = '1Q07l7m9jj4CEHtvuBc0tiQOedNSSz28Sed2EAZShlEc'

# Define a dict 'value_range_body' com base na lista criada anteriormente
value_range_body = {"values": lista}

# Imputando as informações na "Página1" da coluna "A" até a coluna "G"
print('Realizando importação via API...', end=' ', flush=True)
sheet.values().append(spreadsheetId=sheet_id,
                      range="Página1!A:G",
                      valueInputOption='USER_ENTERED',
                      insertDataOption='OVERWRITE',
                      body=value_range_body).execute()

# Define a pasta onde os arquivos CSV serão guardados
processados = r'G:\Drives compartilhados\arquitetura\Documentos\Arquitetura Operacional\BACKOFFICE FINANCEIRO\DASHBOARD\DASH_OPERACIONAL\LOOKER\Arquivos_diarios\PROCESSAMENTO_DOCK\Processados'

# Percorre todos os arquivos na pasta, um por vez
for arquivo in os.listdir(pasta):
# Se o arquivo terminar com '.csv' ou seja, for do tipo csv
    if arquivo.endswith('.csv'):
        # Define o caminho completo do arquivo como uma junção entre o nome da pasta e do arquivo
        pasta_anterior = os.path.join(pasta,arquivo)
        # Define o caminho completo pra onde o arquivo vai como uma junção entre o nome da pasta e do arquivo
        pasta_posterior = os.path.join(processados,arquivo)
        # Renomeia o arquivo fazendo assim a movimentação entre as pastas
        os.rename(pasta_anterior,pasta_posterior)

print('Agrupamento gerado, e arquivos movidos para pasta de processados')