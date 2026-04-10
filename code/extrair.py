import logging
import os
import sqlite3

import pandas as pd
from pysus.online_data import SIH
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


def extrair_dados_datasus(
    estados: list = None,
    anos: list = None,
    meses: list = None,
    caminho_saida: str = "data/raw/sih_to_2023_01.parquet",
) -> pd.DataFrame:
    estados = estados or ["TO"]
    anos = anos or [2023]
    meses = meses or [1]

    logger.info(
        "Iniciando extração SIH — estados=%s | anos=%s | meses=%s",
        estados, anos, meses,
    )

    dados = SIH.download(states=estados, years=anos, months=meses, groups=["RD"])

    if isinstance(dados, list):
        logger.debug("Retorno do PySUS: lista com %d DataFrames — concatenando.", len(dados))
        df = pd.concat(dados, ignore_index=True)
    elif hasattr(dados, "to_dataframe"):
        logger.debug("Retorno do PySUS: ParquetSet — convertendo para DataFrame.")
        df = dados.to_dataframe()
    else:
        logger.debug("Retorno do PySUS: DataFrame direto.")
        df = dados

    logger.info("Extração concluída: %s linhas × %d colunas", f"{df.shape[0]:,}", df.shape[1])

    os.makedirs(os.path.dirname(caminho_saida), exist_ok=True)
    df.to_parquet(caminho_saida, index=False)
    logger.info("Arquivo parquet salvo em: %s", caminho_saida)

    return df


def exportar_para_sql(
    df: pd.DataFrame,
    table_name: str = "sih_internamentos",
    db_path: str = "data/processed/sih_datasus.db",
) -> None:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    engine = create_engine(f"sqlite:///{db_path}")
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    logger.info("Tabela '%s' exportada para o banco: %s", table_name, db_path)

    dump_path = db_path.replace(".db", ".sql")
    with sqlite3.connect(db_path) as conn:
        with open(dump_path, "w", encoding="utf-8") as f:
            for line in conn.iterdump():
                f.write(f"{line}\n")
    logger.info("SQL dump gerado em: %s", dump_path)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    df_raw = extrair_dados_datasus()
    if df_raw is not None:
        exportar_para_sql(df_raw)
