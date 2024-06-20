# Importa biblitecas para tratativa de dados
from googleapiclient.discovery import build
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pandas as pd
import os
import socket    

# Cria listas com os de-para de tipo de moeda (nacional/internacional) e visão (compras/devolucao/daque)
id_tran_nacional_compras = ['4','8','705','706','707','804','809','814','2340','9138','24438','24537']
id_tran_nacional_devolucao = ['25','27','700','821','823','2101','2102','2103','2104','2106','2107',
'2108','2109','2110','2111','8011','8012','8013','8014','8015','8105','8106','8107','8108','8110','9179','24444','24445','24541','24593','24594']
id_tran_nacional_saque = ['9139']

id_tran_internacional_compras = ['3031','9142','9180']
id_tran_internacional_devolucao = ['19281','2122']
id_tran_internacional_saque_cred = ['9153']
id_tran_internacional_saque_deb = ['9212']

# Define a pasta onde os arquivos CSV estão localizados
pasta = (r'G:\Drives compartilhados\arquitetura\Documentos\Arquitetura Operacional\BACKOFFICE FINANCEIRO\DASHBOARD\DASH_OPERACIONAL\LOOKER\Arquivos_diarios\PROCESSAMENTO_DOCK\Bandeira')

# Cria uma Lista vazia para armazenar todos os DataFrames dos arquivos CSV
dfs = []

# Cria a variavel data como vazio
data = ''

# Percorre todos os arquivos na pasta, um por vez
for arquivo in os.listdir(pasta):
    # Se o arquivo terminar com '.csv' ou seja, for do tipo csv
    if arquivo.endswith('.csv'):
        # Define o caminho completo do arquivo como uma junção entre o nome da pasta e do arquivo
        caminho_arquivo = os.path.join(pasta, arquivo)
        # Lê o arquivo CSV
        df_temp = pd.read_csv(caminho_arquivo)
        # Redefine a variavel 'data' como as primeiras 8 letras do nome do arquivo
        data = arquivo[:8]
        # Redefine a variavel 'data' com o formato de data correto
        data = pd.to_datetime(data, format='%d%m%Y')
        # Cria a coluna "DataCompra" com o resultado da variavel data
        df_temp['DataCompra'] = data
        # Adiciona o DataFrame à lista
        dfs.append(df_temp)
        # Redefine a variavel 'data' como as primeiras 8 letras do nome do arquivo
        
# Junta todos os DataFrames em um único DataFrame
df = pd.concat(dfs, ignore_index=True)

# Cria a função que Lê cada uma das linhas para determinar o tipo de transação
def def_moe_clas_tipo(coluna):
    # Se o tipo de transação estiver em 'id_tran_nacional_compras'
    if coluna in id_tran_nacional_compras:
        # Retorna: Nacional, Compras e Credito
        moeda = 'Nacional'
        Classificacao = 'Compras'
        tipo = 'Credito'
        return moeda, Classificacao, tipo
    # Se o tipo de transação estiver em 'id_tran_nacional_devolucao'
    if coluna in id_tran_nacional_devolucao:
        # Retorna: Nacional, Devolução e Debito
        moeda = 'Nacional'
        Classificacao = 'Devolução'
        tipo = 'Debito'
        return moeda, Classificacao, tipo
    # Se o tipo de transação estiver em 'id_tran_nacional_saque'
    if coluna in id_tran_nacional_saque:
        # Retorna: Nacional, Saque e Credito
        moeda = 'Nacional'
        Classificacao = 'Saque'
        tipo = 'Credito'
        return moeda, Classificacao, tipo
    # Se o tipo de transação estiver em 'id_tran_internacional_compras'
    if coluna in id_tran_internacional_compras:
        # Retorna: Internacional, Compras e Credito
        moeda = 'Internacional'
        Classificacao = 'Compras'
        tipo = 'Credito'
        return moeda, Classificacao, tipo
    # Se o tipo de transação estiver em 'id_tran_internacional_devolucao'
    if coluna in id_tran_internacional_devolucao:
        # Retorna: Internacional, Devolução e debito
        moeda = 'Internacional'
        Classificacao = 'Devolução'
        tipo = 'Debito'
        return moeda, Classificacao, tipo
    # Se o tipo de transação estiver em 'id_tran_internacional_saque_cred'
    if coluna in id_tran_internacional_saque_cred:
        # Retorna: Internacional, Saque e Credito
        moeda = 'Internacional'
        Classificacao = 'Saque'
        tipo = 'Credito'
        return moeda, Classificacao, tipo
    # Se o tipo de transação estiver em 'id_tran_internacional_saque_deb'
    if coluna in id_tran_internacional_saque_deb:
        # Retorna: Internacional, saque e Debito
        moeda = 'Internacional'
        Classificacao = 'Saque'
        tipo = 'Debito'
        return moeda, Classificacao, tipo

# Cria a função que Lê cada uma das linhas para determinar o tipo do cartão
def def_tipo_cartao(coluna):
    if 'CRED' in coluna:
        return 'Cartão de Crédito'
    else:
        return 'Cartão de Débito'

# Cria a coluna "Tipo_cartao" com o resultado das validações acima
df['Tipo_cartao'] = df['Produto'].apply(def_tipo_cartao)

# Cria as colunas "Moeda", "Classificação" e "Tipo_transacao" com o resultado das validações acima
df[['Moeda','Classificacao','Tipo_transacao']] = df['Id_TipoTransacao'].apply(lambda x: pd.Series(def_moe_clas_tipo(x)))

# Exclui as linhas em branco, levando em consideração a coluna Moeda
df = df.dropna(subset='Moeda')

# Retira o "R$" do valor, e faz as modificações necessárias para transforma-los em número
df['Valor'] = df['Valor'].str.replace('.','')
df['Valor'] = df['Valor'].str.replace(',','.')
df['Valor'] = df['Valor'].astype(float)

# Redefine a coluna 'Valor' para receber 'Valor' * -1 sempre que a coluna 'Tipo_transacao' for igual a 'Debito'
df.loc[df['Tipo_transacao'] == 'Debito', 'Valor'] = df['Valor']*-1

# Agrupa os valores pelos campos definidos
agrupamento_tipo = df.groupby(['DataCompra','Tipo_transacao','Moeda','Classificacao','Tipo_cartao']).agg(soma=('Valor','sum'),contagem=('Valor','size')).reset_index().round(2)

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