#PARCIONAMENTO DE BIG DATA
#Parcionamento de uma feature gdb. com 89milhões de registro em vários arquivos parquet divididos em 5milhões
#Utilizei time para o controle de quanto tempo demoraria para realizar a análise toda.

import arcpy
import pandas as pd
import pyarrow.parquet as pq
import time
import os

# Caminho da sua feature class
fc = r"C:\caminho.gdb\seu_bigdata"
campos = ["O_LATITUDE", "O_LONGITUDE", "Col_1", "Col_2"]

# Saída
saida_dir = r"C:\seucaminho\PARQUET"
os.makedirs(saida_dir, exist_ok=True)

# Tempo início
t_total_ini = time.time()

print("Iniciando leitura com arcpy...")
t_ini = time.time()
dados = [row for row in arcpy.da.SearchCursor(fc, campos)]
t_fim = time.time()
print(f"Leitura concluída em {t_fim - t_ini:.2f} segundos.")

print("Convertendo para DataFrame...")
t_ini = time.time()
df = pd.DataFrame(dados, columns=campos)
t_fim = time.time()
print(f"DataFrame criado em {t_fim - t_ini:.2f} segundos.")

# Particionamento em blocos de 5 milhões
t_ini = time.time()
total_registros = len(df)
print(f"Total de registros: {total_registros}")
tamanho_bloco = 5_000_000
for i in range(0, total_registros, tamanho_bloco):
    df_chunk = df.iloc[i:i + tamanho_bloco]
    nome_arquivo = f"parte_{i//tamanho_bloco + 1}.parquet" #defina o nome dos seus arquivos. eu coloquei "parte_"
    caminho_saida = os.path.join(saida_dir, nome_arquivo)
    df_chunk.to_parquet(caminho_saida, index=False)
    print(f"Escrita de {nome_arquivo} com {len(df_chunk)} registros.")

t_fim = time.time()
print(f"Escrita total em {t_fim - t_ini:.2f} segundos.")

# Tempo total
t_total_fim = time.time()
print(f"Tempo TOTAL do processo: {t_total_fim - t_total_ini:.2f} segundos.")