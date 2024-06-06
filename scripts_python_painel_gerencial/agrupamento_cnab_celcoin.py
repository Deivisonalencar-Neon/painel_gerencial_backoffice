# Importa as bibliotecas necessárias para o código
import os
import pandas as pd

# Define a pasta onde os arquivos CSV estão localizados
pasta = r'G:\Drives compartilhados\arquitetura\Documentos\Arquitetura Operacional\BACKOFFICE FINANCEIRO\DASHBOARD\DASH_OPERACIONAL\LOOKER\Arquivos_diarios\BOLETOS_CELCOIN'

# Cria uma Lista vazia para armazenar todos os DataFrames dos arquivos CSV
dfs = []

# Percorre todos os arquivos na pasta, um por vez
for arquivo in os.listdir(pasta):
    # Se o arquivo terminar com '.csv' ou seja, for do tipo csv
    if arquivo.endswith('.csv'):
        # Define o caminho completo do arquivo como uma junção entre o nome da pasta e do arquivo
        caminho_arquivo = os.path.join(pasta, arquivo)
        # Lê o arquivo CSV
        df_temp = pd.read_csv(caminho_arquivo, delimiter=";")
        # Adiciona o DataFrame à lista
        dfs.append(df_temp)

# Junta todos os DataFrames em um único DataFrame
df = pd.concat(dfs, ignore_index=True)

# Redefine a coluna 'Data transação' como uma derivada de si mesma(apenas 10 caractres)
df['Data Transação'] = df['Data Transação'].str.slice(0,10)

# Retira o "R$" do valor, e faz as modificações necessárias para transforma-los em número
df['Valor'] = df['Valor'].str.replace('R$ ','')
df['Valor'] = df['Valor'].str.replace('.','')
df['Valor'] = df['Valor'].str.replace(',','.')
df['Valor'] = df['Valor'].astype(float)

# Cria a coluna "DebCre" com o resultado das validações acima
df['CreDeb'] = 'Credito'

# Cria a coluna "Classificaão2" com o texto fixo '-'
df['Classificacao2'] = '-'

# Cria a coluna "Fonte" com o texto fixo 'CNAB CELCOIN'
df['Fonte'] = 'CNAB CELCOIN'

agrupamento_tipo = df.groupby(['Data Transação','CreDeb','Classificacao2','Transação','Fonte']).agg(soma=('Valor','sum'),contagem=('Valor','size')).reset_index().round(2)

# Extrai o resultado para uma planilha especifica
agrupamento_tipo.to_csv(r'C:\Users\U002669\Desktop\teste\validacao_agrupada_celcoin_12_04.csv')