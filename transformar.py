import os
import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
 
 
# CONFIGURAÇÕES
RAW_FILE   = "data/raw/sih_to_2023_01.parquet"
DB_PATH    = "data/processed/sih_datasus.db"
DUMP_PATH  = "data/processed/sih_datasus.sql"
 
os.makedirs("data/processed", exist_ok=True)
 
 
# MAPA: capítulos do CID-10
def categoria_cid(cid: str) -> str:
    if not isinstance(cid, str) or len(cid) < 3:
        return "Desconhecido"
    letra = cid[0].upper()
    try:
        num = int(cid[1:3])
    except ValueError:
        return "Desconhecido"
 
    tabela = {
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
    return tabela.get(letra, "Desconhecido")
 
 
# MAPA: faixa etária
def faixa_etaria(idade) -> str:
    try:
        idade = int(idade)
    except (ValueError, TypeError):
        return "Desconhecida"
    if idade < 2:   return "Lactância"
    if idade <= 4:   return "Primeria infancia"
    if idade <= 10:  return "Segunda infancia"
    if idade <= 13:  return "Pré-adolescência"
    if idade <= 18: return "Adolescência"
    if idade <= 26: return "Pós-adolescência"
    if idade <= 40: return "Adultidade"
    if idade <= 65: return "Meia-idade"
    if idade <= 80: return "Terceira idade"
    return "Quarta Idade"
 
 
# MAPA: região por UF
REGIOES = {
    "AC":"Norte","AM":"Norte","AP":"Norte","PA":"Norte",
    "RO":"Norte","RR":"Norte","TO":"Norte",
    "AL":"Nordeste","BA":"Nordeste","CE":"Nordeste","MA":"Nordeste",
    "PB":"Nordeste","PE":"Nordeste","PI":"Nordeste","RN":"Nordeste","SE":"Nordeste",
    "DF":"Centro-Oeste","GO":"Centro-Oeste","MS":"Centro-Oeste","MT":"Centro-Oeste",
    "ES":"Sudeste","MG":"Sudeste","RJ":"Sudeste","SP":"Sudeste",
    "PR":"Sul","RS":"Sul","SC":"Sul",
}
 
UF_MAP = {
    "11":"RO","12":"AC","13":"AM","14":"RR","15":"PA",
    "16":"AP","17":"TO","21":"MA","22":"PI","23":"CE",
    "24":"RN","25":"PB","26":"PE","27":"AL","28":"SE",
    "29":"BA","31":"MG","32":"ES","33":"RJ","35":"SP",
    "41":"PR","42":"SC","43":"RS","50":"MS","51":"MT",
    "52":"GO","53":"DF",
}
 
def carregar_municipios(caminho_csv: str = "municipios_to.csv") -> dict:
    df = pd.read_csv(caminho_csv, sep=";", dtype=str)
    return dict(zip(df["cod_ibge"], df["nome_municipio"]))
 
# 1. CARREGAR DADOS BRUTOS
def carregar_raw() -> pd.DataFrame:
    print(f"📂 Lendo arquivo bruto: {RAW_FILE}")
    df = pd.read_parquet(RAW_FILE)
    print(f"   {df.shape[0]:,} linhas × {df.shape[1]} colunas carregadas.")
 
    colunas_necessarias = ["N_AIH", "MUNIC_RES", "DIAG_PRINC", "IDADE",
                           "SEXO", "DIAS_PERM", "VAL_TOT", "MORTE",
                           "ANO_CMPT", "MES_CMPT", "DT_INTER", "DT_SAIDA"]
    for col in colunas_necessarias:
        if col not in df.columns:
            print(f"   ⚠️  Coluna '{col}' não encontrada — será criada como nula.")
            df[col] = None
 
    return df
 
 
# 2. CONSTRUIR DIMENSÕES
def build_dim_tempo(df: pd.DataFrame) -> pd.DataFrame:
    print("⚙️  Construindo dim_tempo...")
 
    registros = []
    vistas = set()
 
    for _, row in df[["ANO_CMPT", "MES_CMPT", "DT_INTER"]].drop_duplicates().iterrows():
        try:
            ano  = int(row["ANO_CMPT"]) if pd.notna(row["ANO_CMPT"]) else None
            mes  = int(row["MES_CMPT"])  if pd.notna(row["MES_CMPT"])  else None
 
            dia = None
            if pd.notna(row["DT_INTER"]):
                dt_str = str(row["DT_INTER"]).strip()
                if len(dt_str) == 8 and dt_str.isdigit():
                    dia = int(dt_str[6:8])
                    ano = int(dt_str[0:4])
                    mes = int(dt_str[4:6])
 
            if ano is None or mes is None:
                continue
 
            chave = (ano, mes, dia if dia else 1)
            if chave in vistas:
                continue
            vistas.add(chave)
 
            nomes_mes = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho",
                         "Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]
            registros.append({
                "ano":       ano,
                "mes":       mes,
                "dia":       dia if dia else 1,
                "nome_mes":  nomes_mes[mes - 1] if 1 <= mes <= 12 else "Desconhecido",
                "trimestre": (mes - 1) // 3 + 1,
            })
        except Exception:
            continue
 
    dim = pd.DataFrame(registros).drop_duplicates(subset=["ano", "mes", "dia"])
    dim.insert(0, "sk_tempo", range(1, len(dim) + 1))
    print(f"   {len(dim)} registros em dim_tempo.")
    return dim
 
 
def build_dim_local(df: pd.DataFrame, mapa_municipios: dict) -> pd.DataFrame:
    print("⚙️  Construindo dim_local...")
 
    codigos_unicos = df["MUNIC_RES"].dropna().unique()
    registros = []
    for cod in codigos_unicos:
        cod_str = str(int(float(cod))).zfill(6)
        uf      = UF_MAP.get(cod_str[:2], "??")
        nome    = mapa_municipios.get(cod_str, f"Município {cod_str}")
        regiao  = REGIOES.get(uf, "Desconhecida")
        registros.append({
            "cod_ibge":        cod_str,
            "nome_municipio":  nome,
            "uf":              uf,
            "regiao":          regiao,
        })
 
    dim = pd.DataFrame(registros).drop_duplicates(subset=["cod_ibge"])
    dim.insert(0, "sk_local", range(1, len(dim) + 1))
    print(f"   {len(dim)} registros em dim_local.")
    return dim
 
 
def build_dim_paciente(df: pd.DataFrame) -> pd.DataFrame:
    print("⚙️  Construindo dim_paciente...")
 
    sexo_map = {"1": "Masculino", "3": "Feminino", "0": "Ignorado"}
 
    combinacoes = (
        df[["IDADE", "SEXO"]]
        .dropna(subset=["SEXO"])
        .drop_duplicates()
    )
 
    registros = []
    for _, row in combinacoes.iterrows():
        try:
            idade = int(float(row["IDADE"])) if pd.notna(row["IDADE"]) else -1
        except ValueError:
            idade = -1
        sexo_cod = str(int(float(row["SEXO"]))) if pd.notna(row["SEXO"]) else "0"
        registros.append({
            "idade":       idade if idade >= 0 else None,
            "faixa_etaria": faixa_etaria(idade),
            "sexo":        sexo_map.get(sexo_cod, "Ignorado"),
        })
 
    dim = pd.DataFrame(registros).drop_duplicates(subset=["idade", "sexo"])
    dim.insert(0, "sk_paciente", range(1, len(dim) + 1))
    print(f"   {len(dim)} registros em dim_paciente.")
    return dim
 
 
def build_dim_diagnostico(df: pd.DataFrame) -> pd.DataFrame:
    print("⚙️  Construindo dim_diagnostico...")
 
    cids = df["DIAG_PRINC"].dropna().unique()
    registros = []
    for cid in cids:
        cid_str = str(cid).strip().upper()
        registros.append({
            "cid":           cid_str,
            "descricao_cid": f"CID {cid_str}",   # enriquecer com tabela CID-10 completa
            "categoria_cid": categoria_cid(cid_str),
        })
 
    dim = pd.DataFrame(registros).drop_duplicates(subset=["cid"])
    dim.insert(0, "sk_diag", range(1, len(dim) + 1))
    print(f"   {len(dim)} registros em dim_diagnostico.")
    return dim
 
 
# 3. CONSTRUIR TABELA FATO
def build_fato(df: pd.DataFrame,
               dim_tempo: pd.DataFrame,
               dim_local: pd.DataFrame,
               dim_paciente: pd.DataFrame,
               dim_diag: pd.DataFrame) -> pd.DataFrame:
    print("⚙️  Construindo fato_internacoes...")
 
    trabalho = df.copy()
 
    trabalho["_ano"] = pd.to_numeric(trabalho["ANO_CMPT"], errors="coerce")
    trabalho["_mes"] = pd.to_numeric(trabalho["MES_CMPT"],  errors="coerce")
    trabalho["_dia"] = 1
 
    fato_tempo = dim_tempo.rename(columns={
        "ano": "_ano", "mes": "_mes", "dia": "_dia"
    })[["sk_tempo", "_ano", "_mes", "_dia"]]
    trabalho = trabalho.merge(fato_tempo, on=["_ano", "_mes", "_dia"], how="left")
 
    trabalho["_cod_ibge"] = (
        pd.to_numeric(trabalho["MUNIC_RES"], errors="coerce")
        .dropna()
        .astype(int)
        .astype(str)
        .str.zfill(6)
    )
 
    trabalho["_cod_ibge"] = (
        trabalho["MUNIC_RES"]
        .apply(lambda x: str(int(float(x))).zfill(6) if pd.notna(x) else None)
    )
    fato_local = dim_local[["sk_local", "cod_ibge"]].rename(columns={"cod_ibge": "_cod_ibge"})
    trabalho = trabalho.merge(fato_local, on="_cod_ibge", how="left")
 
    trabalho["_idade"] = pd.to_numeric(trabalho["IDADE"], errors="coerce").fillna(-1).astype(int)
    trabalho["_sexo_cod"] = trabalho["SEXO"].apply(
        lambda x: str(int(float(x))) if pd.notna(x) else "0"
    )
    sexo_map = {"1": "Masculino", "3": "Feminino", "0": "Ignorado"}
    trabalho["_sexo"] = trabalho["_sexo_cod"].map(sexo_map).fillna("Ignorado")
 
    fato_pac = dim_paciente[["sk_paciente", "idade", "sexo"]].copy()
    fato_pac["idade"] = fato_pac["idade"].fillna(-1).astype(int)
    trabalho = trabalho.merge(
        fato_pac.rename(columns={"idade": "_idade", "sexo": "_sexo"}),
        on=["_idade", "_sexo"], how="left"
    )
 
    trabalho["_cid"] = trabalho["DIAG_PRINC"].apply(
        lambda x: str(x).strip().upper() if pd.notna(x) else None
    )
    fato_diag = dim_diag[["sk_diag", "cid"]].rename(columns={"cid": "_cid"})
    trabalho = trabalho.merge(fato_diag, on="_cid", how="left")
 
    trabalho["qt_internacoes"] = 1
    trabalho["qt_obitos"] = pd.to_numeric(trabalho["MORTE"], errors="coerce").fillna(0).astype(int)
    trabalho["vl_total"]   = pd.to_numeric(trabalho["VAL_TOT"], errors="coerce").fillna(0.0)
 
    fato = trabalho[[
        "sk_tempo", "sk_local", "sk_paciente", "sk_diag",
        "qt_internacoes", "qt_obitos", "vl_total"
    ]].copy()
 
    fato = fato.dropna(subset=["sk_tempo", "sk_local", "sk_paciente", "sk_diag"], how="all")
 
    fato.insert(0, "sk_fato", range(1, len(fato) + 1))
    print(f"   {len(fato):,} registros em fato_internacoes.")
    return fato
 
 
# 4. CRIAR SCHEMA SQL NO SQLITE
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
    sk_fato         INTEGER PRIMARY KEY,
    sk_local        INTEGER REFERENCES dim_local(sk_local),
    sk_tempo        INTEGER REFERENCES dim_tempo(sk_tempo),
    sk_paciente     INTEGER REFERENCES dim_paciente(sk_paciente),
    sk_diag         INTEGER REFERENCES dim_diagnostico(sk_diag),
    qt_internacoes  INTEGER,
    qt_obitos       INTEGER,
    vl_total        REAL
);
"""
 
def criar_schema(conn: sqlite3.Connection):
    conn.executescript(DDL)
    conn.commit()
    print("✅ Schema criado no banco de dados.")
 
# 5. CARREGAR TABELAS NO BANCO
def carregar_tabela(df: pd.DataFrame, nome_tabela: str, engine):
    df.to_sql(nome_tabela, engine, if_exists="replace", index=False)
    print(f"   ✅ {nome_tabela}: {len(df):,} linhas carregadas.")
 
 
# 6. GERAR SQL DUMP
def gerar_dump(db_path: str, dump_path: str):
    with sqlite3.connect(db_path) as conn:
        with open(dump_path, "w", encoding="utf-8") as f:
            f.write("-- SQL Dump — Data Mart SIH/DATASUS\n")
            f.write("-- Gerado automaticamente pelo pipeline ETL\n\n")
            for line in conn.iterdump():
                f.write(line + "\n")
    print(f"\n📄 SQL dump salvo em: {dump_path}")
 
 
# 7. EXPORTAR CSVs PARA VISUALIZAÇÃO
def exportar_csvs(tabelas: dict, pasta: str = "data/csv"):
    os.makedirs(pasta, exist_ok=True)
    print(f"\n📊 Exportando CSVs para: {pasta}/")
    for nome, df in tabelas.items():
        caminho = os.path.join(pasta, f"{nome}.csv")
        df.to_csv(caminho, index=False, sep=";", encoding="utf-8-sig")
        print(f"   ✅ {nome}.csv — {len(df):,} linhas")
 
 
# MAIN
def main():
    print("=" * 55)
    print("  ETL — Transformação e Carga no Modelo Dimensional")
    print("=" * 55)
 
    df_raw = carregar_raw()
    mapa_municipios = carregar_municipios("municipios_to.csv")
 
    dim_tempo       = build_dim_tempo(df_raw)
    dim_local       = build_dim_local(df_raw, mapa_municipios)
    dim_paciente    = build_dim_paciente(df_raw)
    dim_diagnostico = build_dim_diagnostico(df_raw)
 
    fato = build_fato(df_raw, dim_tempo, dim_local, dim_paciente, dim_diagnostico)
 
    engine = create_engine(f"sqlite:///{DB_PATH}")
    with sqlite3.connect(DB_PATH) as conn:
        criar_schema(conn)
 
    print("\n📥 Carregando tabelas no banco...")
    carregar_tabela(dim_tempo,       "dim_tempo",       engine)
    carregar_tabela(dim_local,       "dim_local",       engine)
    carregar_tabela(dim_paciente,    "dim_paciente",    engine)
    carregar_tabela(dim_diagnostico, "dim_diagnostico", engine)
    carregar_tabela(fato,            "fato_internacoes", engine)
 
    gerar_dump(DB_PATH, DUMP_PATH)
 
    exportar_csvs({
        "dim_tempo":        dim_tempo,
        "dim_local":        dim_local,
        "dim_paciente":     dim_paciente,
        "dim_diagnostico":  dim_diagnostico,
        "fato_internacoes": fato,
    })
 
    print("\n🎉 Pipeline concluído com sucesso!")
    print(f"   Banco de dados: {DB_PATH}")
    print(f"   SQL dump:       {DUMP_PATH}")
    print(f"   CSVs:           data/csv/")
 
if __name__ == "__main__":
  main()