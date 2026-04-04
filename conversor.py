import pandas as pd
import sqlite3

arquivo_sql = 'sih_datasus.sql'
arquivo_csv = 'dados_extraidos.csv'

conexao = sqlite3.connect(':memory:')

try:
    with open(arquivo_sql, 'r', encoding='utf-8') as f:
        sql_script = f.read()


    conexao.executescript(sql_script)

    # 5. Identificar o nome da tabela que foi criada
    cursor = conexao.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tabelas = cursor.fetchall()
    
    if tabelas:
        nome_tabela = tabelas[0][0] # Pega a primeira tabela encontrada
        print(f"Tabela encontrada: {nome_tabela}")

        # 6. Usar o Pandas para ler a tabela do banco de dados
        df = pd.read_sql_query(f"SELECT * FROM {nome_tabela}", conexao)

        # 7. Salvar para o formato desejado (CSV ou Excel)
        df.to_csv(arquivo_csv, index=False, sep=';', encoding='utf-8-sig')
        print(f"Sucesso! Arquivo convertido em: {arquivo_csv}")
    else:
        print("Nenhuma tabela encontrada no arquivo SQL.")

finally:
    conexao.close()