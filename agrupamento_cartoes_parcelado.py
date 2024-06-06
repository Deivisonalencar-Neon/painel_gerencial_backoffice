# Importa biblitecas para tratativa de dados
import pandas as pd
import os
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
        # Adiciona o DataFrame à lista
        dfs.append(df_temp)
        # Redefine a variavel 'data' como as primeiras 8 letras do nome do arquivo
        data = arquivo[:8]

# Junta todos os DataFrames em um único DataFrame
df = pd.concat(dfs, ignore_index=True)

# Redefine a varivael data com o formato de data correto
data = pd.to_datetime(data, format='%d%m%Y')

# Retira o "R$" do valor, e faz as modificações necessárias para transforma-los em número
df['Total'] = df['Total'].str.replace('R$ ','')
df['Total'] = df['Total'].str.replace('.','')
df['Total'] = df['Total'].str.replace(',','.')
df['Total'] = df['Total'].astype(float)

# Cria a coluna "DataCompra" com o resultado da variavel data
df['DATA'] = data

# Cria a coluna "DebCre" com o resultado das validações acima
df['DEBCRED'] = 'Credito'

# Cria as coluna "Classificação1" com o texto fixo 'Nacional'
df['CLASSIFICAÇÃO1'] = 'Nacional'

# Cria as coluna "Classificação2" com o texto fixo 'Compras parceladas'
df['CLASSIFICAÇÃO2'] = 'Compras parceladas'

# Cria a coluna "Fonte" com o texto fixo 'Cartão de crédito'
df['FONTE'] = 'Cartão de Crédito'

# Agrupa os valores pelos campos definidos
agrupamento_tipo = df.groupby(['DATA','DEBCRED','CLASSIFICAÇÃO1','CLASSIFICAÇÃO2','FONTE'])['Total'].sum().round(2)

# Extrai o resultado para uma planilha especifica
print(agrupamento_tipo)