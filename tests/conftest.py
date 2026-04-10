"""
Fixtures compartilhadas entre todos os testes.

Gera dados sintéticos que imitam o formato real do DATASUS/SIH,
sem depender de conexão de rede ou arquivos externos.
"""
import sqlite3
import tempfile
import os

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# DADO BRUTO SINTÉTICO
# ---------------------------------------------------------------------------

MUNICIPIOS_MOCK = {
    "172100": "Palmas",
    "171020": "Araguaína",
    "170210": "Guaraí",
    "999999": "Município Desconhecido",
}


def _make_raw_df(n: int = 100) -> pd.DataFrame:
    """Cria um DataFrame que replica o schema do SIH/DATASUS."""
    import numpy as np
    rng = np.random.default_rng(42)

    municipios = ["172100", "171020", "170210", "172100", "171020"]
    cids = ["A01", "B20", "C34", "J18", "K35", "O80", "I10", "R51", "T14", "Z00"]
    sexos = [1, 3, 0]

    return pd.DataFrame({
        "N_AIH":      [f"AIH{str(i).zfill(10)}" for i in range(n)],
        "MUNIC_RES":  rng.choice(municipios, n),
        "DIAG_PRINC": rng.choice(cids, n),
        "IDADE":      rng.integers(0, 90, n),
        "SEXO":       rng.choice(sexos, n),
        "DIAS_PERM":  rng.integers(1, 30, n),
        "VAL_TOT":    rng.uniform(100, 5000, n).round(2),
        "MORTE":      rng.choice([0, 1], n, p=[0.95, 0.05]),
        "ANO_CMPT":   [2023] * n,
        "MES_CMPT":   rng.integers(1, 12, n),
        "DT_INTER":   ["20230115"] * n,
        "DT_SAIDA":   ["20230120"] * n,   # sempre após DT_INTER
    })


@pytest.fixture(scope="session")
def df_raw():
    """DataFrame bruto com 100 linhas sintéticas e dados válidos."""
    return _make_raw_df(100)


@pytest.fixture(scope="session")
def df_raw_com_problemas():
    """DataFrame bruto com problemas propositais para testar validações."""
    df = _make_raw_df(50)
    # Introduz problemas controlados
    df.loc[0:4, "IDADE"] = -5                    # idades negativas
    df.loc[5:9, "DT_INTER"] = "99999999"         # datas inválidas
    df.loc[10:14, "MUNIC_RES"] = None            # nulos em coluna crítica
    df.loc[15:19, "DT_SAIDA"] = "20220101"       # saída antes da entrada
    df.loc[20:24, "SEXO"] = 9                    # sexo desconhecido
    return df


@pytest.fixture(scope="session")
def mapa_municipios():
    return MUNICIPIOS_MOCK.copy()


# ---------------------------------------------------------------------------
# DIMENSÕES E FATO CONSTRUÍDOS A PARTIR DO RAW SINTÉTICO
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def dims_e_fato(df_raw, mapa_municipios):
    """Roda as funções de build_ sobre o dataset sintético e retorna tudo."""
    from code.transformar import (
        build_dim_tempo,
        build_dim_local,
        build_dim_paciente,
        build_dim_diagnostico,
        build_fato,
    )

    dim_tempo  = build_dim_tempo(df_raw)
    dim_local  = build_dim_local(df_raw, mapa_municipios)
    dim_pac    = build_dim_paciente(df_raw)
    dim_diag   = build_dim_diagnostico(df_raw)
    fato       = build_fato(df_raw, dim_tempo, dim_local, dim_pac, dim_diag)

    return {
        "dim_tempo":       dim_tempo,
        "dim_local":       dim_local,
        "dim_paciente":    dim_pac,
        "dim_diagnostico": dim_diag,
        "fato":            fato,
    }


# ---------------------------------------------------------------------------
# BANCO SQLITE EM MEMÓRIA COM STAR SCHEMA POPULADO
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def sqlite_conn_populado(dims_e_fato):
    """Cria um banco SQLite em memória com o Star Schema completamente carregado."""
    from code.transformar import criar_schema
    from sqlalchemy import create_engine

    conn = sqlite3.connect(":memory:")
    criar_schema(conn)

    engine = create_engine("sqlite://", creator=lambda: conn)

    for nome, df in [
        ("dim_tempo",       dims_e_fato["dim_tempo"]),
        ("dim_local",       dims_e_fato["dim_local"]),
        ("dim_paciente",    dims_e_fato["dim_paciente"]),
        ("dim_diagnostico", dims_e_fato["dim_diagnostico"]),
        ("fato_internacoes", dims_e_fato["fato"]),
    ]:
        df.to_sql(nome, engine, if_exists="replace", index=False)

    yield conn
    conn.close()


@pytest.fixture()
def sqlite_conn_vazio():
    """Banco SQLite em memória sem nenhuma tabela."""
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# ARQUIVO PARQUET TEMPORÁRIO
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def parquet_path(df_raw, tmp_path_factory):
    """Salva df_raw em parquet e devolve o caminho — para testes de integração."""
    tmp = tmp_path_factory.mktemp("data")
    path = str(tmp / "sih_test.parquet")
    df_raw.to_parquet(path, index=False)
    return path
