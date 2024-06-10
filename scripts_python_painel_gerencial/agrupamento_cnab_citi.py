# Importa as bibliotecas necessárias para o código
import os
import csv
import pandas as pd

# Função para extrair colunas por número de caracteres
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
pasta = r'G:\Drives compartilhados\arquitetura\Documentos\Arquitetura Operacional\BACKOFFICE FINANCEIRO\DASHBOARD\DASH_OPERACIONAL\LOOKER\Arquivos_diarios\BOLETOS_CITI'

# Define o tamanho de cada coluna
tamanhos_colunas = [1,2,14,20,25,2,12,6,25,1,2,6,10,2,12,6,6,13,3,5,2,13,13,13,13,13,13,13,13,8,8,6,20,13,3,2,20,6,3,1,14,1,1,6,3,6] # Ajuste conforme necessário para simetrik

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
        df_temp = pd.DataFrame(colunas, columns=['Coluna1','Coluna2','Coluna3','Classificacao1','Coluna5','Coluna6','Coluna7','Coluna8','Coluna9','Coluna10','Coluna11','Coluna12','Coluna13','Coluna14','Coluna15','Coluna16','Coluna17','Coluna18','Coluna19','Coluna20','Coluna21','Coluna22','Coluna23','Coluna24','Coluna25','Coluna26','Coluna27','Valor','Coluna29','Coluna30','Coluna31','Data','Coluna33','Coluna34','Coluna35','Coluna36','Coluna37','Coluna38','Coluna39','Coluna40','Coluna41','Coluna42','Coluna43','Coluna44','Coluna45','Coluna46'])
        
        # Filtra apenas os valores onde a coluna11 é igual a 06 ou 17
        df_temp = df_temp.loc[(df_temp['Coluna11'] == '06') | (df_temp['Coluna11'] == '17')]

        # Ajusta a coluna de data, identificando seu formato e corrigindo para o formato correto
        df_temp['Data'] = pd.to_datetime(df_temp['Data'], format='%d%m%y')
        df_temp['Data'] = df_temp['Data'].dt.date

        # Ajusta a valor transformando espaços em branco em zero e transforma em float
        df_temp['Valor'] = df_temp['Valor'].replace('','0')
        df_temp['Valor'] = df_temp['Valor'].astype(float) / -100

        # Cria a coluna "DebCre" com o resultado das validações acima
        df_temp['CreDeb'] = 'Credito'
        
        # Redefine as coluna "Classificação1" com o texto fixo 'Con' juntamente com seu proprio valor
        df_temp['Classificacao1'] = 'Con' + df_temp['Classificacao1']

        # Cria as coluna "Classificação2" com o texto fixo '-'
        df_temp['Classificacao2'] = '-'

        # Cria a coluna "Fonte" com o texto fixo 'CNAB CITI'
        df_temp['Fonte'] = 'CNAB CITI'

        # Adiciona o DataFrame à lista
        dfs.append(df_temp)

# Junta todos os DataFrames em um único DataFrame
df = pd.concat(dfs, ignore_index=True)

# Agrupa os valores pelos campos definidos
agrupamento_tipo = df.groupby(['Data','CreDeb','Classificacao1','Classificacao2','Fonte']).agg(soma=('Valor','sum'),contagem=('Valor','size')).reset_index().round(2)

# Extrai o resultado para uma planilha especifica
agrupamento_tipo.to_csv(r'C:\Users\U002669\Downloads\validacao_agrupada_cnab_citi.csv')

# Define a pasta onde os arquivos CSV serão guardados
processados = r'G:\Drives compartilhados\arquitetura\Documentos\Arquitetura Operacional\BACKOFFICE FINANCEIRO\DASHBOARD\DASH_OPERACIONAL\LOOKER\Arquivos_diarios\BOLETOS_CITI\Processados'

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