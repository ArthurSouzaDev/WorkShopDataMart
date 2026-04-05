import os
import pandas as pd
#!pip install pysus pandas fastparquet # Install missing dependencies
from pysus.online_data import SIH
from sqlalchemy import create_engine # pip install SQLAlchemy
import sqlite3 # Built-in Python module

def extrair_dados_datasus():
    # 1. Definir os parâmetros de extração
    estados = ['TO'] # Tocantins (podem adicionar mais estados à lista)
    anos = [2023]    # Ano escolhido
    meses = [1]      # Janeiro

    print(f"A descarregar dados de internamento (SIH) para {estados} no período {meses}/{anos}...")

    # 2. Fazer o download dos dados
    # O grupo 'RD' refere-se à AIH Reduzida (dados principais de internamento hospitalar)
    # O PySUS vai ao servidor FTP do Governo, descarrega o .dbc e converte para nós
    dados = SIH.download(states=estados, years=anos, months=meses, groups=['RD'])

    # Dependendo da versão, o PySUS pode devolver um DataFrame, uma lista de DataFrames ou um ParquetSet
    if isinstance(dados, list):
        df_internamentos = pd.concat(dados, ignore_index=True)
    elif hasattr(dados, 'to_dataframe'): # Se for um ParquetSet ou similar com método to_dataframe()
        df_internamentos = dados.to_dataframe()
    else:
        df_internamentos = dados # Assume que já é um DataFrame ou similar

    # 3. Pequena exploração para garantir que funcionou
    print("\n✅ Extração concluída com sucesso!")
    print(f"Total de internamentos registados (Linhas): {df_internamentos.shape[0]}")
    print(f"Total de colunas (Atributos): {df_internamentos.shape[1]}")

    # Mostrar apenas algumas colunas chave para o nosso futuro Modelo Dimensional
    colunas_interesse = ['N_AIH', 'MUNIC_RES', 'DIAG_PRINC', 'IDADE', 'SEXO', 'DIAS_PERM', 'VAL_TOT']
    print("\nPrimeiras linhas dos dados brutos:")
    print(df_internamentos[colunas_interesse].head())

    # 4. Guardar os dados brutos na camada Raw
    # Criar a pasta se não existir
    os.makedirs('data/raw', exist_ok=True)

    # Guardar em formato Parquet (muito mais leve, rápido e profissional que o CSV)
    caminho_ficheiro = 'data/raw/sih_to_2023_01.parquet'
    df_internamentos.to_parquet(caminho_ficheiro, index=False)

    print(f"\n📁 Ficheiro guardado na rota: {caminho_ficheiro}")

    return df_internamentos

def exportar_para_sql(df, table_name='sih_internamentos', db_path='data/processed/sih_datasus.db'):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Criar um motor de banco de dados SQLite
    engine = create_engine(f'sqlite:///{db_path}')

    # Exportar o DataFrame para uma tabela no SQLite
    # 'if_exists="replace"' vai recriar a tabela cada vez que for executado
    df.to_sql(table_name, engine, if_exists='replace', index=False)

    print(f"\n✅ Dados exportados para a tabela '{table_name}' no banco de dados SQLite: {db_path}")

    # Opcional: Gerar um SQL dump a partir do arquivo SQLite
    # Isso cria um arquivo .sql com as instruções CREATE TABLE e INSERT
    dump_file_path = db_path.replace('.db', '.sql')
    with sqlite3.connect(db_path) as conn:
        with open(dump_file_path, 'w') as f:
            for line in conn.iterdump():
                f.write(f'{line}\n')
    print(f"\n📄 SQL dump gerado em: {dump_file_path}")

# Executar a função de extração e depois a de exportação
if __name__ == "__main__":
    df_raw_data = extrair_dados_datasus()
    if df_raw_data is not None:
        exportar_para_sql(df_raw_data)