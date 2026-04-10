import logging
import sqlite3

import pandas as pd

logger = logging.getLogger(__name__)


def converter_sql_para_csv(
    caminho_sql: str = "data/processed/sih_datasus.sql",
    caminho_csv: str = "code/dados_extraidos.csv",
) -> None:
    logger.info("Iniciando conversão: %s → %s", caminho_sql, caminho_csv)

    conexao = sqlite3.connect(":memory:")
    try:
        with open(caminho_sql, "r", encoding="utf-8") as f:
            sql_script = f.read()

        conexao.executescript(sql_script)

        cursor = conexao.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tabelas = cursor.fetchall()

        if not tabelas:
            raise ValueError(f"Nenhuma tabela encontrada em '{caminho_sql}'.")

        if len(tabelas) > 1:
            logger.warning(
                "Múltiplas tabelas encontradas (%s). Usando a primeira: '%s'.",
                [t[0] for t in tabelas],
                tabelas[0][0],
            )

        nome_tabela = tabelas[0][0]
        logger.info("Tabela selecionada para exportação: '%s'", nome_tabela)

        df = pd.read_sql_query(f"SELECT * FROM {nome_tabela}", conexao)
        logger.info("Lidas %s linhas da tabela '%s'", f"{len(df):,}", nome_tabela)

        df.to_csv(caminho_csv, index=False, sep=";", encoding="utf-8-sig")
        logger.info("CSV salvo em: %s (%s linhas)", caminho_csv, f"{len(df):,}")

    finally:
        conexao.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    converter_sql_para_csv()
