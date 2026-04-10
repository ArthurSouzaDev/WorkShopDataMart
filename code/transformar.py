import logging
import os
import sqlite3

import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# MAPAS DE DOMÍNIO
# ---------------------------------------------------------------------------

NOMES_MES = [
    "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
    "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro",
]

REGIOES = {
    "AC": "Norte", "AM": "Norte", "AP": "Norte", "PA": "Norte",
    "RO": "Norte", "RR": "Norte", "TO": "Norte",
    "AL": "Nordeste", "BA": "Nordeste", "CE": "Nordeste", "MA": "Nordeste",
    "PB": "Nordeste", "PE": "Nordeste", "PI": "Nordeste", "RN": "Nordeste", "SE": "Nordeste",
    "DF": "Centro-Oeste", "GO": "Centro-Oeste", "MS": "Centro-Oeste", "MT": "Centro-Oeste",
    "ES": "Sudeste", "MG": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
    "PR": "Sul", "RS": "Sul", "SC": "Sul",
}

UF_MAP = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA",
    "16": "AP", "17": "TO", "21": "MA", "22": "PI", "23": "CE",
    "24": "RN", "25": "PB", "26": "PE", "27": "AL", "28": "SE",
    "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP",
    "41": "PR", "42": "SC", "43": "RS", "50": "MS", "51": "MT",
    "52": "GO", "53": "DF",
}

CATEGORIAS_CID = {
    "A": "Doenças infecciosas e parasitárias",
    "B": "Doenças infecciosas e parasitárias",
    "C": "Neoplasias",
    "D": "Neoplasias / Doenças do sangue",
    "E": "Doenças endócrinas e metabólicas",
    "F": "Transtornos mentais e comportamentais",
    "G": "Doenças do sistema nervoso",
    "H": "Doenças dos olhos / ouvidos",
    "I": "Doenças do aparelho circulatório",
    "J": "Doenças do aparelho respiratório",
    "K": "Doenças do aparelho digestivo",
    "L": "Doenças da pele",
    "M": "Doenças do sistema osteomuscular",
    "N": "Doenças do aparelho geniturinário",
    "O": "Gravidez, parto e puerpério",
    "P": "Afecções perinatais",
    "Q": "Malformações congênitas",
    "R": "Sintomas e sinais anormais",
    "S": "Lesões e causas externas",
    "T": "Lesões e causas externas",
    "V": "Causas externas de morbidade",
    "W": "Causas externas de morbidade",
    "X": "Causas externas de morbidade",
    "Y": "Causas externas de morbidade",
    "Z": "Fatores que influenciam o estado de saúde",
}

SEXO_MAP = {"1": "Masculino", "3": "Feminino", "0": "Ignorado"}

# ---------------------------------------------------------------------------
# FUNÇÕES AUXILIARES
# ---------------------------------------------------------------------------

def categoria_cid(cid: str) -> str:
    if not isinstance(cid, str) or len(cid) < 3:
        return "Desconhecido"
    letra = cid[0].upper()
    try:
        int(cid[1:3])
    except ValueError:
        return "Desconhecido"
    return CATEGORIAS_CID.get(letra, "Desconhecido")


def faixa_etaria(idade) -> str:
    try:
        idade = int(idade)
    except (ValueError, TypeError):
        return "Desconhecida"
    if idade < 2:   return "Lactância"
    if idade <= 4:  return "Primeira infância"
    if idade <= 10: return "Segunda infância"
    if idade <= 13: return "Pré-adolescência"
    if idade <= 18: return "Adolescência"
    if idade <= 26: return "Pós-adolescência"
    if idade <= 40: return "Adultidade"
    if idade <= 65: return "Meia-idade"
    if idade <= 80: return "Terceira idade"
    return "Quarta idade"


def carregar_municipios(caminho_csv: str = "code/municipios.csv") -> dict:
    df = pd.read_csv(caminho_csv, sep=";", dtype=str)
    mapa = dict(zip(df["cod_ibge"], df["nome_municipio"]))
    logger.info("Municipios carregados: %d entradas de '%s'", len(mapa), caminho_csv)
    return mapa


# ---------------------------------------------------------------------------
# 1. CARREGAR DADOS BRUTOS
# ---------------------------------------------------------------------------

def carregar_raw(raw_file: str) -> pd.DataFrame:
    logger.info("Lendo arquivo bruto: %s", raw_file)
    df = pd.read_parquet(raw_file)
    logger.info("Arquivo carregado: %s linhas × %d colunas", f"{df.shape[0]:,}", df.shape[1])

    colunas_necessarias = [
        "N_AIH", "MUNIC_RES", "DIAG_PRINC", "IDADE",
        "SEXO", "DIAS_PERM", "VAL_TOT", "MORTE",
        "ANO_CMPT", "MES_CMPT", "DT_INTER", "DT_SAIDA",
    ]
    ausentes = [c for c in colunas_necessarias if c not in df.columns]
    if ausentes:
        logger.warning("Colunas ausentes no raw (serão criadas como nulas): %s", ausentes)
        for col in ausentes:
            df[col] = pd.NA
    return df


# ---------------------------------------------------------------------------
# 2. CONSTRUIR DIMENSÕES
# ---------------------------------------------------------------------------

def build_dim_tempo(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Construindo dim_tempo...")
    registros = [{"ano": None, "mes": None, "dia": None, "nome_mes": "Desconhecido", "trimestre": None}]
    vistas: set = set()
    invalidos = 0

    for dt in df["DT_INTER"].dropna().unique():
        try:
            dt_str = str(dt).strip()
            if len(dt_str) != 8 or not dt_str.isdigit():
                invalidos += 1
                continue
            ano, mes, dia = int(dt_str[:4]), int(dt_str[4:6]), int(dt_str[6:8])
            chave = (ano, mes, dia)
            if chave in vistas:
                continue
            vistas.add(chave)
            registros.append({
                "ano": ano,
                "mes": mes,
                "dia": dia,
                "nome_mes": NOMES_MES[mes - 1] if 1 <= mes <= 12 else "Desconhecido",
                "trimestre": (mes - 1) // 3 + 1 if 1 <= mes <= 12 else None,
            })
        except Exception:
            invalidos += 1

    if invalidos:
        logger.warning("dim_tempo: %d datas inválidas ignoradas.", invalidos)

    dim = pd.DataFrame(registros).reset_index(drop=True)
    dim.insert(0, "sk_tempo", range(len(dim)))
    logger.info("dim_tempo criada: %d registros.", len(dim))
    return dim


def build_dim_local(df: pd.DataFrame, mapa_municipios: dict) -> pd.DataFrame:
    logger.info("Construindo dim_local...")
    registros = [{"cod_ibge": None, "nome_municipio": "Desconhecido", "uf": None, "regiao": "Desconhecida"}]
    vistas: set = set()
    invalidos = 0

    for cod in df["MUNIC_RES"].dropna().unique():
        try:
            cod_str = str(int(float(cod))).zfill(6)
            if len(cod_str) != 6 or not cod_str.isdigit():
                invalidos += 1
                continue
            if cod_str in vistas:
                continue
            vistas.add(cod_str)
            uf = UF_MAP.get(cod_str[:2])
            registros.append({
                "cod_ibge": cod_str,
                "nome_municipio": mapa_municipios.get(cod_str, f"Município {cod_str}"),
                "uf": uf,
                "regiao": REGIOES.get(uf, "Desconhecida"),
            })
        except Exception:
            invalidos += 1

    if invalidos:
        logger.warning("dim_local: %d códigos IBGE inválidos ignorados.", invalidos)

    dim = pd.DataFrame(registros).reset_index(drop=True)
    dim.insert(0, "sk_local", range(len(dim)))
    logger.info("dim_local criada: %d registros.", len(dim))
    return dim


def build_dim_paciente(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Construindo dim_paciente...")
    registros = [{"idade": None, "faixa_etaria": "Desconhecida", "sexo": "Ignorado"}]
    vistas: set = set()
    invalidos = 0

    for _, row in df[["IDADE", "SEXO"]].drop_duplicates().iterrows():
        try:
            idade = int(float(row["IDADE"])) if pd.notna(row["IDADE"]) else None
            if idade is not None and idade < 0:
                idade = None
            sexo_cod = str(int(float(row["SEXO"]))) if pd.notna(row["SEXO"]) else "0"
            sexo = SEXO_MAP.get(sexo_cod, "Ignorado")
            chave = (idade, sexo)
            if chave in vistas:
                continue
            vistas.add(chave)
            registros.append({
                "idade": idade,
                "faixa_etaria": faixa_etaria(idade) if idade is not None else "Desconhecida",
                "sexo": sexo,
            })
        except Exception:
            invalidos += 1

    if invalidos:
        logger.warning("dim_paciente: %d registros inválidos ignorados.", invalidos)

    dim = pd.DataFrame(registros).reset_index(drop=True)
    dim.insert(0, "sk_paciente", range(len(dim)))
    logger.info("dim_paciente criada: %d registros.", len(dim))
    return dim


def build_dim_diagnostico(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Construindo dim_diagnostico...")
    registros = [{"cid": None, "descricao_cid": "Desconhecido", "categoria_cid": "Desconhecido"}]
    vistas: set = set()
    invalidos = 0

    for cid in df["DIAG_PRINC"].unique():
        try:
            if pd.isna(cid):
                continue
            cid_str = str(cid).strip().upper()
            if len(cid_str) < 3 or not cid_str[0].isalpha() or not cid_str[1:3].isdigit():
                invalidos += 1
                continue
            if cid_str in vistas:
                continue
            vistas.add(cid_str)
            registros.append({
                "cid": cid_str,
                "descricao_cid": f"CID {cid_str}",
                "categoria_cid": categoria_cid(cid_str),
            })
        except Exception:
            invalidos += 1

    if invalidos:
        logger.warning("dim_diagnostico: %d códigos CID inválidos ignorados.", invalidos)

    dim = pd.DataFrame(registros).reset_index(drop=True)
    dim.insert(0, "sk_diag", range(len(dim)))
    logger.info("dim_diagnostico criada: %d registros.", len(dim))
    return dim


# ---------------------------------------------------------------------------
# 3. CONSTRUIR TABELA FATO
# ---------------------------------------------------------------------------

def build_fato(
    df: pd.DataFrame,
    dim_tempo: pd.DataFrame,
    dim_local: pd.DataFrame,
    dim_paciente: pd.DataFrame,
    dim_diag: pd.DataFrame,
) -> pd.DataFrame:
    logger.info("Construindo fato_internacoes...")
    trabalho = df.copy()

    # --- join tempo ---
    dt_inter = trabalho["DT_INTER"].astype("string").str.strip()
    dt_valida = dt_inter.str.fullmatch(r"\d{8}", na=False)
    trabalho[["_ano", "_mes", "_dia"]] = None

    trabalho.loc[dt_valida, "_ano"] = pd.to_numeric(dt_inter[dt_valida].str[0:4], errors="coerce")
    trabalho.loc[dt_valida, "_mes"] = pd.to_numeric(dt_inter[dt_valida].str[4:6], errors="coerce")
    trabalho.loc[dt_valida, "_dia"] = pd.to_numeric(dt_inter[dt_valida].str[6:8], errors="coerce")

    fato_tempo = dim_tempo.rename(columns={"ano": "_ano", "mes": "_mes", "dia": "_dia"})[
        ["sk_tempo", "_ano", "_mes", "_dia"]
    ]
    trabalho = trabalho.merge(fato_tempo, on=["_ano", "_mes", "_dia"], how="left")
    sk_tempo_unk = dim_tempo.loc[dim_tempo["ano"].isna(), "sk_tempo"].iloc[0]
    nao_mapeados_tempo = trabalho["sk_tempo"].isna().sum()
    if nao_mapeados_tempo:
        logger.warning("fato: %s registros sem mapeamento de tempo (→ desconhecido).", f"{nao_mapeados_tempo:,}")
    trabalho["sk_tempo"] = trabalho["sk_tempo"].fillna(sk_tempo_unk)

    # --- join local ---
    def safe_cod_ibge(x):
        try:
            return str(int(float(x))).zfill(6)
        except Exception:
            return None

    trabalho["_cod_ibge"] = trabalho["MUNIC_RES"].apply(safe_cod_ibge)
    fato_local = dim_local[["sk_local", "cod_ibge"]].rename(columns={"cod_ibge": "_cod_ibge"})
    trabalho = trabalho.merge(fato_local, on="_cod_ibge", how="left")
    sk_local_unk = dim_local.loc[dim_local["cod_ibge"].isna(), "sk_local"].iloc[0]
    nao_mapeados_local = trabalho["sk_local"].isna().sum()
    if nao_mapeados_local:
        logger.warning("fato: %s registros sem mapeamento de local (→ desconhecido).", f"{nao_mapeados_local:,}")
    trabalho["sk_local"] = trabalho["sk_local"].fillna(sk_local_unk)

    # --- join paciente ---
    def safe_sexo(x):
        try:
            return str(int(float(x)))
        except Exception:
            return "0"

    trabalho["_idade"] = pd.to_numeric(trabalho["IDADE"], errors="coerce").fillna(-1).astype(int)
    trabalho["_sexo"] = trabalho["SEXO"].apply(safe_sexo).map(SEXO_MAP).fillna("Ignorado")

    fato_pac = dim_paciente[["sk_paciente", "idade", "sexo"]].copy()
    fato_pac["idade"] = fato_pac["idade"].fillna(-1).astype(int)
    trabalho = trabalho.merge(
        fato_pac.rename(columns={"idade": "_idade", "sexo": "_sexo"}),
        on=["_idade", "_sexo"],
        how="left",
    )
    sk_pac_unk = dim_paciente.loc[dim_paciente["idade"].isna(), "sk_paciente"].iloc[0]
    nao_mapeados_pac = trabalho["sk_paciente"].isna().sum()
    if nao_mapeados_pac:
        logger.warning("fato: %s registros sem mapeamento de paciente (→ desconhecido).", f"{nao_mapeados_pac:,}")
    trabalho["sk_paciente"] = trabalho["sk_paciente"].fillna(sk_pac_unk)

    # --- join diagnostico ---
    def safe_cid(x):
        if pd.isna(x):
            return None
        cid = str(x).strip().upper()
        if len(cid) < 3 or not cid[0].isalpha() or not cid[1:3].isdigit():
            return None
        return cid

    trabalho["_cid"] = trabalho["DIAG_PRINC"].apply(safe_cid)
    fato_diag = dim_diag[["sk_diag", "cid"]].rename(columns={"cid": "_cid"})
    trabalho = trabalho.merge(fato_diag, on="_cid", how="left")
    sk_diag_unk = dim_diag.loc[dim_diag["cid"].isna(), "sk_diag"].iloc[0]
    nao_mapeados_diag = trabalho["sk_diag"].isna().sum()
    if nao_mapeados_diag:
        logger.warning("fato: %s registros sem mapeamento de diagnóstico (→ desconhecido).", f"{nao_mapeados_diag:,}")
    trabalho["sk_diag"] = trabalho["sk_diag"].fillna(sk_diag_unk)

    # --- métricas ---
    trabalho["qt_internacoes"] = 1
    trabalho["qt_obitos"] = pd.to_numeric(trabalho["MORTE"], errors="coerce").fillna(0).astype(int)
    trabalho["dias_permanencia"] = pd.to_numeric(trabalho["DIAS_PERM"], errors="coerce").fillna(0).astype(int)

    fato = trabalho[[
        "sk_tempo", "sk_local", "sk_paciente", "sk_diag",
        "qt_internacoes", "qt_obitos", "dias_permanencia",
    ]].copy()
    fato.insert(0, "sk_fato", range(1, len(fato) + 1))

    logger.info("fato_internacoes criada: %s registros.", f"{len(fato):,}")
    return fato


# ---------------------------------------------------------------------------
# 4. DDL
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS dim_tempo (
    sk_tempo   INTEGER PRIMARY KEY,
    dia        INTEGER,
    mes        INTEGER,
    nome_mes   TEXT,
    trimestre  INTEGER,
    ano        INTEGER
);
CREATE TABLE IF NOT EXISTS dim_local (
    sk_local        INTEGER PRIMARY KEY,
    cod_ibge        TEXT,
    nome_municipio  TEXT,
    uf              TEXT,
    regiao          TEXT
);
CREATE TABLE IF NOT EXISTS dim_paciente (
    sk_paciente  INTEGER PRIMARY KEY,
    idade        INTEGER,
    faixa_etaria TEXT,
    sexo         TEXT
);
CREATE TABLE IF NOT EXISTS dim_diagnostico (
    sk_diag       INTEGER PRIMARY KEY,
    cid           TEXT,
    descricao_cid TEXT,
    categoria_cid TEXT
);
CREATE TABLE IF NOT EXISTS fato_internacoes (
    sk_fato          INTEGER PRIMARY KEY,
    sk_diag          INTEGER REFERENCES dim_diagnostico(sk_diag),
    sk_local         INTEGER REFERENCES dim_local(sk_local),
    sk_tempo         INTEGER REFERENCES dim_tempo(sk_tempo),
    sk_paciente      INTEGER REFERENCES dim_paciente(sk_paciente),
    qt_internacoes   INTEGER,
    qt_obitos        INTEGER,
    dias_permanencia INTEGER
);
"""


def criar_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL)
    conn.commit()
    logger.info("Schema criado no banco de dados.")


def carregar_tabela(df: pd.DataFrame, nome_tabela: str, engine) -> None:
    df.to_sql(nome_tabela, engine, if_exists="replace", index=False)
    logger.info("Tabela '%s' carregada: %s linhas.", nome_tabela, f"{len(df):,}")


def gerar_dump(db_path: str, dump_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        with open(dump_path, "w", encoding="utf-8") as f:
            f.write("-- SQL Dump — Data Mart SIH/DATASUS\n")
            f.write("-- Gerado automaticamente pelo pipeline ETL\n\n")
            for line in conn.iterdump():
                f.write(line + "\n")
    logger.info("SQL dump salvo em: %s", dump_path)


def exportar_csvs(tabelas: dict, pasta: str = "data/csv") -> None:
    os.makedirs(pasta, exist_ok=True)
    for nome, df in tabelas.items():
        caminho = os.path.join(pasta, f"{nome}.csv")
        df.to_csv(caminho, index=False, sep=";", encoding="utf-8-sig")
        logger.info("CSV exportado: %s (%s linhas)", caminho, f"{len(df):,}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main(
    raw_file: str = "./data/raw/sih_to_2023_01.parquet",
    db_path: str = "./data/processed/sih_datasus.db",
    dump_path: str = "./data/processed/sih_datasus.sql",
    municipios_csv: str = "code/municipios.csv",
    csv_pasta: str = "data/csv",
    relatorio_qualidade: str = "data/relatorio_qualidade.csv",
) -> None:
    from code.validar import validate_raw, validate_dimensional, salvar_relatorio

    logger.info("=" * 55)
    logger.info("  ETL — Transformação e Carga no Modelo Dimensional")
    logger.info("=" * 55)

    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    df_raw = carregar_raw(raw_file)

    # --- validação pré-carga ---
    logger.info("Executando validação do dado bruto...")
    report_raw = validate_raw(df_raw)
    if report_raw.tem_erros:
        logger.error(
            "Dado bruto possui erros críticos de qualidade. "
            "Corrija os problemas antes de prosseguir. Resumo: %s",
            report_raw.resumo(),
        )
        raise ValueError("Validação do dado bruto falhou com erros críticos.")

    mapa_municipios = carregar_municipios(municipios_csv)

    dim_tempo       = build_dim_tempo(df_raw)
    dim_local       = build_dim_local(df_raw, mapa_municipios)
    dim_paciente    = build_dim_paciente(df_raw)
    dim_diagnostico = build_dim_diagnostico(df_raw)
    fato            = build_fato(df_raw, dim_tempo, dim_local, dim_paciente, dim_diagnostico)

    engine = create_engine(f"sqlite:///{db_path}")
    with sqlite3.connect(db_path) as conn:
        criar_schema(conn)

    logger.info("Carregando tabelas no banco...")
    carregar_tabela(dim_tempo,       "dim_tempo",        engine)
    carregar_tabela(dim_local,       "dim_local",        engine)
    carregar_tabela(dim_paciente,    "dim_paciente",     engine)
    carregar_tabela(dim_diagnostico, "dim_diagnostico",  engine)
    carregar_tabela(fato,            "fato_internacoes", engine)

    # --- validação pós-carga ---
    logger.info("Executando validação do modelo dimensional...")
    with sqlite3.connect(db_path) as conn:
        report_dim = validate_dimensional(conn)

    salvar_relatorio(report_raw, report_dim, relatorio_qualidade)

    if report_dim.tem_erros:
        logger.error(
            "Modelo dimensional possui erros de integridade. "
            "Verifique: %s", relatorio_qualidade,
        )

    gerar_dump(db_path, dump_path)
    exportar_csvs(
        {
            "dim_tempo":        dim_tempo,
            "dim_local":        dim_local,
            "dim_paciente":     dim_paciente,
            "dim_diagnostico":  dim_diagnostico,
            "fato_internacoes": fato,
        },
        pasta=csv_pasta,
    )

    logger.info("Pipeline concluído! Resumo qualidade — raw: %s | dim: %s",
                report_raw.resumo(), report_dim.resumo())
    logger.info("Banco: %s | Dump: %s | CSVs: %s/", db_path, dump_path, csv_pasta)
    logger.info("Relatório de qualidade: %s", relatorio_qualidade)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    main()
