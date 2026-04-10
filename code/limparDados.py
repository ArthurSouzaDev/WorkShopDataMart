import logging

import pandas as pd

logger = logging.getLogger(__name__)

COLUNAS_FOCO = [
    "ANO_CMPT", "MES_CMPT", "DT_INTER", "DT_SAIDA", "NASC",
    "SEXO", "IDADE", "DIAG_PRINC", "DIAS_PERM", "MORTE",
    "VAL_TOT", "MUNIC_RES", "RACA_COR", "CNES",
]

MAPA_SEXO = {1: "Masculino", 3: "Feminino"}
MAPA_RACA = {
    1: "Branca",
    2: "Preta",
    3: "Parda",
    4: "Amarela",
    5: "Indígena",
    99: "Sem Informação",
}


def _ajustar_data(coluna: pd.Series) -> pd.Series:
    return pd.to_datetime(coluna.astype(str), format="%Y%m%d", errors="coerce")


def limpar_dados(
    caminho_entrada: str = "code/dados_extraidos.csv",
    caminho_saida: str = "code/saude_bi_final.csv",
) -> pd.DataFrame:
    logger.info("Carregando dados de: %s", caminho_entrada)
    df = pd.read_csv(caminho_entrada, sep=";", low_memory=False)
    logger.info("Dataset carregado: %s linhas × %d colunas", f"{df.shape[0]:,}", df.shape[1])

    colunas_ausentes = [c for c in COLUNAS_FOCO if c not in df.columns]
    if colunas_ausentes:
        logger.warning("Colunas ausentes no dataset (serão ignoradas): %s", colunas_ausentes)

    colunas_disponiveis = [c for c in COLUNAS_FOCO if c in df.columns]
    df_limpo = df[colunas_disponiveis].copy()
    logger.info("Selecionadas %d colunas para o BI", len(colunas_disponiveis))

    for col_data in ["DT_INTER", "DT_SAIDA", "NASC"]:
        if col_data not in df_limpo.columns:
            continue
        validos_antes = df_limpo[col_data].notna().sum()
        df_limpo[col_data] = _ajustar_data(df_limpo[col_data])
        perdidos = validos_antes - df_limpo[col_data].notna().sum()
        if perdidos > 0:
            logger.warning(
                "Coluna '%s': %s datas inválidas convertidas para nulo",
                col_data, f"{perdidos:,}",
            )

    df_limpo["SEXO_DESC"] = df_limpo["SEXO"].map(MAPA_SEXO).fillna("Não Informado")
    df_limpo["INDICADOR_OBITO"] = df_limpo["MORTE"].apply(lambda x: "Sim" if x > 0 else "Não")
    df_limpo["RACA_DESC"] = df_limpo["RACA_COR"].map(MAPA_RACA).fillna("Sem Informação")

    if "DT_INTER" in df_limpo.columns and "NASC" in df_limpo.columns:
        df_limpo["IDADE_ANOS"] = (df_limpo["DT_INTER"] - df_limpo["NASC"]).dt.days // 365
        logger.debug("Coluna IDADE_ANOS calculada a partir de DT_INTER e NASC.")

    nulos = df_limpo.isnull().sum()
    colunas_com_nulos = nulos[nulos > 0]
    if not colunas_com_nulos.empty:
        logger.info("Resumo de valores nulos:")
        for col, n in colunas_com_nulos.items():
            pct = n / len(df_limpo) * 100
            logger.info("  %-20s %s nulos (%.1f%%)", col, f"{n:,}", pct)

    df_limpo.to_csv(caminho_saida, index=False, sep=";", encoding="utf-8-sig")
    logger.info(
        "Dados limpos salvos em: %s (%s linhas, %d colunas)",
        caminho_saida, f"{len(df_limpo):,}", len(df_limpo.columns),
    )
    return df_limpo


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    limpar_dados()
