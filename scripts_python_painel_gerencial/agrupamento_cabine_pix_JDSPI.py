# Importa biblitecas para tratativa de dados
import os
import pandas as pd

# Define a pasta onde os arquivos CSV estão localizados
pasta = r'\\sgnfiles.neon.local\Compartilhado\CEC\ConciliationFiles\ConciliationInput\Processed\2024\06.JUNE\20240609'

# Cria uma Lista vazia para armazenar todos os DataFrames dos arquivos CSV
dfs = []

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
df['Data'] = df['Data'].dt.date

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

# Extrai o resultado para uma planilha especifica
agrupamento_tipo.to_csv(r'C:\Users\U002669\Downloads\validacao_agrupada_pix_09_06.csv')

print('Arquivo gerado')