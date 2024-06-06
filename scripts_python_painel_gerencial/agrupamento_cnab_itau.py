# Importa as bibliotecas necessárias para o código
import os
import csv
import pandas as pd

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
        df_temp['Data'] = df_temp['Data'].dt.date
        
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

# Extrai o resultado para uma planilha especifica
agrupamento_tipo.to_csv(r'C:\Users\U002669\Downloads\validacao_agrupada_cnab_itau.csv')