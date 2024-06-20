# Importa biblitecas para tratativa de dados
from googleapiclient.discovery import build
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pandas as pd
import os
import socket
import csv

# Define a pasta onde os arquivos CSV estão localizados
pasta = r'G:\Drives compartilhados\arquitetura\Documentos\Arquitetura Operacional\BACKOFFICE FINANCEIRO\DASHBOARD\DASH_OPERACIONAL\LOOKER\Arquivos_diarios\CDB_TOPAZ'

# Cria uma Lista vazia para armazenar todos os DataFrames dos arquivos CSV
dfs = []

# Percorre todos os arquivos na pasta, um por vez
for arquivo in os.listdir(pasta):
    # Se o arquivo terminar com '.csv' ou seja, for do tipo csv
    if arquivo.endswith('.csv'):
        # Define o caminho completo do arquivo como uma junção entre o nome da pasta e do arquivo
        caminho_arquivo = os.path.join(pasta, arquivo)
        # Lê o arquivo CSV, pegando apenas as colunas que serão utilizadas
        df_temp = pd.read_csv(caminho_arquivo, delimiter=';', usecols=[3,12,15,18,22,24])
        # Adiciona o DataFrame à lista
        dfs.append(df_temp)

# Junta todos os DataFrames em um único DataFrame
df = pd.concat(dfs, ignore_index=True)

# Retira o "R$" do valor, e faz as modificações necessárias para transforma-los em número
df['Valor'] = df['Valor'].str.replace('.','')
df['Valor'] = df['Valor'].str.replace(',','.')
df['Valor'] = df['Valor'].astype(float)

# Retira o "R$" do valor_liquido, e faz as modificações necessárias para transforma-los em número
df['Valor Liquido'] = df['Valor Liquido'].str.replace('.','')
df['Valor Liquido'] = df['Valor Liquido'].str.replace(',','.')
df['Valor Liquido'] = df['Valor Liquido'].astype(float)

# Cria a função que Lê cada uma das linhas para determinar o tipo de registro
def definir_tipo_registro(Valor):
    if Valor >= 5000:
        return 'Clearing'
    elif Valor < 5000:
        return 'Aplicativo'

# Cria a coluna "Classificacao" com o resultado das validações acima
df['Classificacao'] = df['Valor'].apply(definir_tipo_registro)

# Redefine a coluna 'Valor' para receber 'Valor liquido' sempre que a coluna 'tipo oper.' for igual a resgate
df.loc[df['Tipo Oper.'] == 'Resgate', 'Valor'] = df['Valor Liquido']

# Redefine a coluna 'investidor' para receber o texto fixo 'VAREJO' sempre que a coluna 'Liquidação' for igual a 'Conta-Corrente'
df.loc[df['Liquidacao'] == 'Conta-Corrente', 'Investidor'] = "VAREJO"

# Redefine a coluna 'Tipo Oper.' para receber o texto fixo 'Aplicação' sempre que a coluna 'Tipo Oper' for igual a 'Emissão' 
df.loc[df['Tipo Oper.'] == 'Emissão', 'Tipo Oper.'] = "Aplicação"

# Cria a coluna "Fonte" com o texto fixo 'CDB TOPAZ'
df['Fonte'] = 'CDB TOPAZ'

# Agrupa os valores pelos campos definidos
agrupamento_tipo = df.groupby(['Data Lançamento','Tipo Oper.','Investidor','Classificacao','Fonte']).agg(soma=('Valor','sum'),contagem=('Valor','size')).reset_index().round(2)

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
processados = r'G:\Drives compartilhados\arquitetura\Documentos\Arquitetura Operacional\BACKOFFICE FINANCEIRO\DASHBOARD\DASH_OPERACIONAL\LOOKER\Arquivos_diarios\CDB_TOPAZ\Processados'

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