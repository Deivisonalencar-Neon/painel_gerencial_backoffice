import pandas as pd
import os

pasta = r'G:\Drives compartilhados\arquitetura\Documentos\Arquitetura Operacional\BACKOFFICE FINANCEIRO\DASHBOARD\DASH_OPERACIONAL\LOOKER\Arquivos_diarios\BOLETOS_CELCOIN'

dfs = []

for arquivo in os.listdir(pasta):
    if arquivo.endswith('.xlsx'):
        caminho_arquivo = os.path.join(pasta,arquivo)
        df_temp = pd.read_excel(caminho_arquivo)
        dfs.append(df_temp)

df = pd.concat(dfs, ignore_index=True)

print('Agrupamento gerado, e arquivos movidos para pasta de processados')
