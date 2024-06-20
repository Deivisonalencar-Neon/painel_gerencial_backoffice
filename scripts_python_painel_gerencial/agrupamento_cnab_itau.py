# Importa as bibliotecas necessárias para o código
from googleapiclient.discovery import build
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pandas as pd
import os
import socket 

# Cria a Função para extrair colunas por número de caracteres
def extrair_colunas(linhas, tamanhos):
    # Cria uma lista vazia para colunas
    colunas = []
    # Percorre toda as linhas uma por vez:
    for linha in linhas:
        # Cria uma lista vazia para uma coluna
        coluna = []
        # Define o inicio como 0 (primeiro caracter)
        inicio = 0
        # Percorre todos os tamanhos, uma por vez:
        for tamanho in tamanhos:
            #Adiciona a lista coluna apenas os caracteres especificos daque tamanho
            coluna.append(linha[inicio:inicio + tamanho].strip())
            # Redefina o inicio, como sendo o inicio anterior + o tamanho já percorrido
            inicio += tamanho
        # Adiciona os valores da coluna na lista de colunas
        colunas.append(coluna)
    # Retorna a lista colunas:
    return colunas

# Define a pasta onde os arquivos CSV estão localizados
pasta = r'G:\Drives compartilhados\arquitetura\Documentos\Arquitetura Operacional\BACKOFFICE FINANCEIRO\DASHBOARD\DASH_OPERACIONAL\LOOKER\Arquivos_diarios\BOLETOS_ITAU'

# Define o tamanho de cada coluna
tamanhos_colunas = [1, 4, 7, 7, 14, 24, 8, 8, 6, 3, 10, 2, 44, 13, 4, 9, 4, 2, 21, 9]

# Cria uma Lista vazia para armazenar todos os DataFrames dos arquivos CSV
dfs = []

# Percorre todos os arquivos na pasta, um por vez
for arquivo in os.listdir(pasta):
    # Se o arquivo terminar com '.RET' ou seja, for do tipo RET
    if arquivo.endswith('.RET'):
        # Define o caminho completo do arquivo como uma junção entre o nome da pasta e do arquivo
        caminho_arquivo = os.path.join(pasta, arquivo)
        # Le linhas do arquivo de entrada
        with open(caminho_arquivo, 'r') as arquivo:
            linhas = arquivo.readlines()
        # Redefine a lista colunas, com o resultado da função de extrair colunas
        colunas = extrair_colunas(linhas, tamanhos_colunas)
        # Cria um dataframe apenas com as colunas importantes e as renomeia
        df_temp = pd.DataFrame(colunas, columns=['Coluna1','Coluna2','Coluna3','Coluna4','Coluna5','Coluna6','Data','Coluna8','Coluna9','Coluna10','Coluna11','Coluna12','Coluna13','Coluna14','Coluna15','Coluna16','Coluna17','Coluna18','Coluna19','Coluna20'])
        
        # Ajusta a coluna14 (valor) transformando espaços em branco em zero e transforma em float
        df_temp['Coluna14'] = df_temp['Coluna14'].replace('','0')
        df_temp['Coluna14'] = df_temp['Coluna14'].astype(float) / 100

        # Filtra linhas onde Coluna5 é o CNPJ da Neon (para retirar linhas em branco ou com dados indesejaveis)
        df_temp = df_temp.loc[(df_temp['Coluna5'] == '20855875000182')]

        # Ajusta a coluna de data, identificando seu formato e corrigindo para o formato correto
        df_temp['Data'] = pd.to_datetime(df_temp['Data'], format='%Y%m%d')
        df_temp['Data'] = df_temp['Data'].dt.strftime(r'%d/%m/%Y')
        
        # Cria a coluna "DebCre" com o resultado das validações acima
        df_temp['CreDeb'] = 'Debito'

        # Cria as coluna "Classificação1" e "Classificação2" com o texto fixo '-'
        df_temp['Classificacao1'] = '-'
        df_temp['Classificacao2'] = '-'
        
        # Cria a coluna "Fonte" com o texto fixo 'CNAB ITAU'
        df_temp['Fonte'] = 'CNAB ITAU'

        # Adiciona o DataFrame à lista
        dfs.append(df_temp)

# Junta todos os DataFrames em um único DataFrame
df = pd.concat(dfs, ignore_index=True)

# Agrupa os valores pelos campos definidos
agrupamento_tipo = df.groupby(['Data','CreDeb','Classificacao1','Classificacao2','Fonte']).agg(soma=('Coluna14','sum'),contagem=('Coluna14','size')).reset_index().round(2)

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
processados = r'G:\Drives compartilhados\arquitetura\Documentos\Arquitetura Operacional\BACKOFFICE FINANCEIRO\DASHBOARD\DASH_OPERACIONAL\LOOKER\Arquivos_diarios\BOLETOS_ITAU\Processados'

# Percorre todos os arquivos na pasta, um por vez
for arquivo in os.listdir(pasta):
# Se o arquivo terminar com '.RET' ou seja, for do tipo csv
    if arquivo.endswith('.RET'):
        # Define o caminho completo do arquivo como uma junção entre o nome da pasta e do arquivo
        pasta_anterior = os.path.join(pasta,arquivo)
        # Define o caminho completo pra onde o arquivo vai como uma junção entre o nome da pasta e do arquivo
        pasta_posterior = os.path.join(processados,arquivo)
        # Renomeia o arquivo fazendo assim a movimentação entre as pastas
        os.rename(pasta_anterior,pasta_posterior)

print('Agrupamento gerado, e arquivos movidos para pasta de processados')