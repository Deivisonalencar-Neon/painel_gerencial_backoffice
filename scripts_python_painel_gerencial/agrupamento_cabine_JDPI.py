# Importa biblitecas para tratativa de dados
import pandas as pd
import os

# Define variavies utilizadas nas proximas linhas
des_tib = "73 - Ressarcimento de Custos Operacionais - RCO"
des_rep = "85 - Acerto entre instituições financeiras por suspeita ou identificação de fraude"
des_cust = '121 - Pagamento da TIR - Pix Saque e/ou Pix Troco'
cnpj_neon = "20.855.875/0001-82"
conta_neon = "165514"

# Define a pasta onde os arquivos CSV estão localizados
pasta = r'G:\Drives compartilhados\arquitetura\Documentos\Arquitetura Operacional\BACKOFFICE FINANCEIRO\DASHBOARD\DASH_OPERACIONAL\LOOKER\Arquivos_diarios\CABINE_IP'

# Cria uma Lista vazia para armazenar todos os DataFrames dos arquivos CSV
dfs = []

# Percorre todos os arquivos na pasta, um por vez
for arquivo in os.listdir(pasta):
    # Se o arquivo terminar com '.csv' ou seja, for do tipo csv
    if arquivo.endswith('.csv'):
        # Define o caminho completo do arquivo como uma junção entre o nome da pasta e do arquivo
        caminho_arquivo = os.path.join(pasta, arquivo)
        # Lê o arquivo CSV, pegando apenas as colunas que serão utilizadas
        df_temp = pd.read_csv(caminho_arquivo, encoding='latin-1', delimiter=';', usecols=[0,1,5,9,10,15,16,19,20,21,22,23,24])
        # Adiciona o DataFrame à lista
        dfs.append(df_temp)

# Junta todos os DataFrames em um único DataFrame
df = pd.concat(dfs, ignore_index=True)

# Cria a função que Lê cada uma das linhas para determinar o tipo de transação
def def_tipo_transacao(row):
    # Se a situação da mensagem for rejeitata pelo Bacen ou pela IF, retorna 'NAO USUAL'
    if (row['Situação Msg'] == 'Rejeitado Bacen' or row['Rejeitado IF'] == ''):
        return 'NAO USUAL'
    
    # Se a mensagem for do tipo 'SLB0002', retorna 'CUSTO STR'
    elif (row['Mensagem'] == "SLB0002"):
        return 'CUSTO STR'

    # Se a mensagem for do tipo 'SLB0005', retorna 'CLIENTES CREDITO'
    elif (row['Mensagem'] == "SLB0005"):
        return 'CLIENTES CREDITO'

    # Se a mensagem for do tipo "STR0010" ou a mensagem teminar com "R2" e a situação da mensagem for 'Devolvido', retorna 'Devolução Credito'
    elif (row['Mensagem'] == "STR0010" or (str(row['Mensagem'])[-2:] == "R2") and row['Situação Msg'] == 'Devolvido'):
        return 'DEVOLUCOES - CREDITO'
    
    # Se a mensagem for do tipo "STR0010R2" ou a mensagem não teminar com "R2" e a situação da mensagem for 'Devolvido', retorna 'Devolução Credito'
    elif (row['Mensagem'] == "STR0010R2" or (str(row['Mensagem'])[-2:] != "R2") and row['Situação Msg'] == 'Devolvido'):
        return 'DEVOLUCOES - DEBITO'
    
    # Se a mensagem for do tipo "STR0004R2" e a finalidade for "73" retorna "TIB credito"
    elif (row['Mensagem'] == "STR0004R2" and row['Finalidade'] == des_tib):
        return 'TIB - CREDITO'
    
    # Se a mensagem for do tipo "STR0004" e a finalidade for "73" retorna "TIB debito"
    elif (row['Mensagem'] == "STR0004" and row['Finalidade'] == des_tib):
        return 'TIB - DEBITO'

    # Se a mensagem for do tipo "STR0004" e a finalidade for "73" retorna "TIB debito"
    elif (row['Mensagem'] == "STR0004" and row['Finalidade'] == des_cust):
        return 'CUSTO - PIX SQ TROCO'
    
    # Se a mensagem for do tipo "STR0004R2" e a finalidade for "85" retorna "Repatriação credito"
    elif (row['Mensagem'] == "STR0004R2" and row['Finalidade'] == des_rep):
        return 'REPATRIACAO - CREDITO'
    
    # Se a mensagem for do tipo "STR0004" e a finalidade for "85" retorna "Repatriação debito"
    elif (row['Mensagem'] == "STR0004" and row['Finalidade'] == des_rep):
        return 'REPATRIACAO - DEBITO'
    
    # Se a mensagem for do tipo "STR0025R2" retorna "Judicial credito"
    elif row['Mensagem'] == "STR0025R2":
        return 'JUDICIAL - CREDITO'
    
    # Se a mensagem for do tipo "STR0025" retorna "Judicial debito"
    elif row['Mensagem'] == "STR0025":
        return 'JUDICIAL - DEBITO'
    
    # Se a mensagem terminar com "R2", o CNPJ creditado for igual ao cnpj da neon, e a conta creditada não for a conta principal, retorna "Transitoria Credito"
    elif str(row['Mensagem'])[-2:] == "R2" and (row['CPF/CNPJ Creditado'] == cnpj_neon) and (row['Conta Creditada'] != 165514):
        return 'TRANSITORIA CREDITO'
    
    # Se a mensagem não terminar com "R2", o CNPJ debitado for igual ao cnpj da neon, e a conta creditada não for a conta principal, retorna "Transitoria Debito"
    elif str(row['Mensagem'])[-2:] != "R2" and (row['CPF/CNPJ Debitado'] == cnpj_neon) and (row['Conta Debitada'] != 165514):
        return 'TRANSITORIA DEBITO'
    
    # Se a mensagem terminar com "R2" retorna "Clientes credito"
    elif str(row['Mensagem'])[-2:] == "R2":
        return 'CLIENTES CREDITO'
    
    # Se a mensagem não terminar com "R2" retorna "Clientes debito"
    elif str(row['Mensagem'])[-2:] != "R2":
        return 'CLIENTES DEBITO'
    
    # Caso nenhuma das situações acima for verdadeira, retorna "erro"
    else:
        return 'erro'

# Cria a coluna "Tipo" com o resultado das validações acima
df['Tipo'] = df.apply(def_tipo_transacao, axis=1)

# Define a coluna valor como texto
df['Valor'].astype(str)

# Retira o "R$" do valor, e faz as modificações necessárias para transforma-los em número
df['Valor'] = df['Valor'].str.replace('R$ ','')
df['Valor'] = df['Valor'].str.replace('.','')
df['Valor'] = df['Valor'].str.replace(',','.')
df['Valor'] = df['Valor'].astype(float)

# Cria a função que Lê cada uma das linhas para determinar o tipo de transação (debito ou credito)
def definir_deb_cred(Valor):
    if Valor >= 0:
        return 'Credito'
    elif Valor < 0:
        return 'Debito'

# Cria a coluna "DebCre" com o resultado das validações acima
df['Debcre'] = df['Valor'].apply(definir_deb_cred)

# Exclui as linhas em branco, levando em consideração a coluna grupo
df = df.dropna(subset=['Grupo'])

# Cria a coluna "Fonte" com o texto fixo 'Cabine JDIP'
df['Fonte'] = 'Cabine JDIP'

# Caso seja necessário identificar como cada transação foi classificada, retirar comentario abaixo: 
#df.to_csv(r'incluir um caminho')

# Agrupa os valores pelos campos definidos
agrupamento_tipo = df.groupby(['Data Msg','Debcre','Mensagem','Tipo','Fonte']).agg(soma=('Valor','sum'),contagem=('Valor','size')).reset_index().round(2)

# Extrai o resultado para uma planilha especifica
agrupamento_tipo.to_csv(r'C:\Users\U002669\Desktop\teste\validacao_agrupada_jd_12_042.csv')