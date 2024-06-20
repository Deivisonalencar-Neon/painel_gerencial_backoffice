# Importa biblitecas para tratativa de dados
from googleapiclient.discovery import build
from google.oauth2 import service_account
from datetime import datetime, timedelta
import pandas as pd
import os
import socket    

# Define o inicio padrão da pasta onde os arquivos CSV estão localizados
caminho = r'\\sgnfiles.neon.local\Compartilhado\CEC\ConciliationFiles\ConciliationInput\Processed'

# Define qual o data de hoje
hoje = datetime.today()

# Define qual dia da semana de hoje
dia_semana_hoje = hoje.weekday()

#Alterar para o dia da semana, caso seja feriado:
#hoje = datetime.today() - timedelta(days=1)
#dia_semana_hoje = hoje.weekday()

# Define data como a data de hoje
data = hoje

# se o dia da semana for segunda-feira (0)
if dia_semana_hoje == 0:
    # Define data como a data de dois dias atrás (d-2)
    data = hoje - timedelta(days=2)

# Cria uma Lista vazia para armazenar todos os nomes das pastas que serão lidas
pastas = []

# Enquanto a data for menor ou igual a data de hoje:
while data <= hoje:
    # define o dia da data
    data_dia = str(data.strftime('%d'))
    # define o mes da data
    data_mes = str(data.strftime('%m'))
    # define o nome do mes da data
    data_nome_mes = str(data.strftime('%B')).upper()
    # define o ano da data
    data_ano = str(data.strftime('%Y'))
    # define a data completa
    data_completa = str(data.strftime('%Y%m%d'))
    # define a junção entre o mes e o seu nome
    data_mes_pasta = data_mes + '.' + data_nome_mes

    # define o caminho completo da pasta, sendo o 'caminho padrão\ano\junção entre mes e seu nome\data completa'
    # Exemplo: \\sgnfiles.neon.local\Compartilhado\CEC\ConciliationFiles\ConciliationInput\Processed\2024\06.JUNE\20240618
    caminho_completo = os.path.join(caminho,data_ano,data_mes_pasta,data_completa)

    # Adiciona o caminho completo a lista de pastas
    pastas.append(caminho_completo)

    # Redefine data como sendo ela mesmo + 1 dia 
    data = data + timedelta(days=1)

# Cria uma Lista vazia para armazenar todos os DataFrames dos arquivos CSV
dfs = []

# Percorre todas pastas na lista de pastas, um po vez
for pasta in pastas:
    # Percorre todos os arquivos na pasta, um por vez
    for arquivo in os.listdir(pasta):
        # Se o arquivo terminar com '.csv' ou seja, for do tipo csv
        if arquivo.endswith('.csv'):
            # Define o caminho completo do arquivo como uma junção entre o nome da pasta e do arquivo
            caminho_arquivo = os.path.join(pasta, arquivo)
            # Lê o arquivo CSV
            df_temp = pd.read_csv(caminho_arquivo)
            # Adiciona o DataFrame à lista
            dfs.append(df_temp)

    # Junta todos os DataFrames em um único DataFrame
    df = pd.concat(dfs, ignore_index=True)

# Cria a coluna 'Data' como uma derivada da coluna 'MovementDate', identificando um formato especifico
df['Data'] = pd.to_datetime(df['MovementDate'], format=r'%Y-%m-%d %H:%M:%S')

# Ajusta a coluna 'Data' no formato de data correto
df['Data'] = df['Data'].dt.strftime(r'%d/%m/%Y')

# Cria a função que Lê cada uma das linhas para determinar o tipo de transação (debito ou credito)
def definir_deb_cred(Value):
    if Value >= 0:
        return 'Credito'
    elif Value < 0:
        return 'Debito'

# Cria a coluna "DebCre" com o resultado das validações acima
df['Debcre'] = df['Value'].apply(definir_deb_cred)

# Cria a coluna "Classificaão2" com o texto fixo '-'
df['CLASSIFICAÇÃO2'] = '-'

# Cria a coluna "Fonte" com o texto fixo 'PIX JD'
df['Fonte']  = 'PIX JD'

# Agrupa os valores pelos campos definidos
agrupamento_tipo = df.groupby(['Data','Debcre','CLASSIFICAÇÃO2','Fonte','OperationDescription']).agg(soma=('Value','sum'),contagem=('Value','size')).reset_index().round(2)

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
