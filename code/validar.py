"""
Módulo de validação e qualidade de dados — Data Mart SIH/DATASUS

Executa dois blocos de validação:

1. validate_raw(df)         — valida o DataFrame bruto logo após a extração
2. validate_dimensional(conn) — valida o banco dimensional após a carga

Ambos retornam um ValidationReport com a lista de checks e um DataFrame
de métricas que pode ser salvo como CSV.
"""
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# ESTRUTURAS DE RESULTADO
# ---------------------------------------------------------------------------

@dataclass
class CheckResult:
    categoria: str
    check: str
    status: str          # "OK" | "AVISO" | "ERRO"
    detalhe: str
    valor: Any = None


@dataclass
class ValidationReport:
    checks: list[CheckResult] = field(default_factory=list)

    def add(self, categoria: str, check: str, status: str, detalhe: str, valor: Any = None) -> None:
        r = CheckResult(categoria, check, status, detalhe, valor)
        self.checks.append(r)
        log_fn = logger.error if status == "ERRO" else (logger.warning if status == "AVISO" else logger.info)
        log_fn("[%s] %s — %s: %s", status, categoria, check, detalhe)

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame([
            {
                "categoria": c.categoria,
                "check": c.check,
                "status": c.status,
                "detalhe": c.detalhe,
                "valor": c.valor,
            }
            for c in self.checks
        ])

    @property
    def tem_erros(self) -> bool:
        return any(c.status == "ERRO" for c in self.checks)

    @property
    def tem_avisos(self) -> bool:
        return any(c.status == "AVISO" for c in self.checks)

    def resumo(self) -> str:
        ok = sum(1 for c in self.checks if c.status == "OK")
        av = sum(1 for c in self.checks if c.status == "AVISO")
        er = sum(1 for c in self.checks if c.status == "ERRO")
        return f"{len(self.checks)} checks — OK: {ok} | AVISOS: {av} | ERROS: {er}"


# ---------------------------------------------------------------------------
# LIMITES CONFIGURÁVEIS
# ---------------------------------------------------------------------------

LIMIAR_NULOS_CRITICOS = 0.10       # > 10% de nulos em coluna crítica → ERRO
LIMIAR_NULOS_SECUNDARIOS = 0.30    # > 30% de nulos em coluna secundária → AVISO
LIMIAR_DESCONHECIDOS_FATO = 0.05   # > 5% de FKs mapeadas p/ "desconhecido" → AVISO
LIMIAR_LETIDADE_MAXIMA = 0.30      # taxa de mortalidade > 30% → AVISO
IDADE_MAXIMA_PLAUSIVEL = 130
DIAS_PERM_MAXIMOS_PLAUSIVEL = 1000
ANO_MINIMO = 1990
ANO_MAXIMO = date.today().year + 1

COLUNAS_CRITICAS = ["N_AIH", "MUNIC_RES", "DIAG_PRINC", "IDADE", "SEXO", "DT_INTER"]
COLUNAS_METRICAS = ["DIAS_PERM", "MORTE", "VAL_TOT"]


# ---------------------------------------------------------------------------
# 1. VALIDAÇÃO DO DADO BRUTO
# ---------------------------------------------------------------------------

def validate_raw(df: pd.DataFrame) -> ValidationReport:
    """Valida o DataFrame bruto extraído do DATASUS antes da transformação."""
    report = ValidationReport()
    n = len(df)
    logger.info("Iniciando validação do dado bruto (%s linhas)...", f"{n:,}")

    # --- completude ---
    report.add("Completude", "Total de linhas", "OK", f"{n:,} registros carregados", n)

    for col in COLUNAS_CRITICAS:
        if col not in df.columns:
            report.add("Completude", f"Coluna '{col}' existe", "ERRO",
                       f"Coluna obrigatória ausente no dataset")
            continue
        nulos = df[col].isna().sum()
        pct = nulos / n
        status = "OK" if pct <= LIMIAR_NULOS_CRITICOS else "ERRO"
        report.add(
            "Completude", f"Nulos em '{col}' (crítica)",
            status, f"{nulos:,} nulos ({pct:.1%})", round(pct, 4),
        )

    for col in COLUNAS_METRICAS:
        if col not in df.columns:
            report.add("Completude", f"Coluna '{col}' existe", "AVISO",
                       "Coluna de métrica ausente")
            continue
        nulos = df[col].isna().sum()
        pct = nulos / n
        status = "OK" if pct <= LIMIAR_NULOS_SECUNDARIOS else "AVISO"
        report.add(
            "Completude", f"Nulos em '{col}' (métrica)",
            status, f"{nulos:,} nulos ({pct:.1%})", round(pct, 4),
        )

    # --- duplicatas ---
    if "N_AIH" in df.columns:
        dupes = df["N_AIH"].duplicated().sum()
        status = "OK" if dupes == 0 else "AVISO"
        report.add("Unicidade", "Duplicatas em N_AIH", status,
                   f"{dupes:,} AIHs duplicadas", dupes)

    # --- validade de datas ---
    if "DT_INTER" in df.columns:
        dt_str = df["DT_INTER"].astype(str).str.strip()
        formato_invalido = (~dt_str.str.fullmatch(r"\d{8}", na=True)).sum()
        status = "OK" if formato_invalido == 0 else "AVISO"
        report.add("Formato", "Formato de DT_INTER (YYYYMMDD)", status,
                   f"{formato_invalido:,} datas com formato inválido", formato_invalido)

    if "DT_INTER" in df.columns and "DT_SAIDA" in df.columns:
        di = pd.to_numeric(df["DT_INTER"], errors="coerce")
        ds = pd.to_numeric(df["DT_SAIDA"], errors="coerce")
        invertidas = ((di.notna()) & (ds.notna()) & (ds < di)).sum()
        status = "OK" if invertidas == 0 else "ERRO"
        report.add("Consistência", "DT_SAIDA >= DT_INTER", status,
                   f"{invertidas:,} registros com data de saída antes da internação", invertidas)

    # --- faixa de ano ---
    if "ANO_CMPT" in df.columns:
        anos = pd.to_numeric(df["ANO_CMPT"], errors="coerce")
        fora_faixa = ((anos < ANO_MINIMO) | (anos > ANO_MAXIMO)).sum()
        status = "OK" if fora_faixa == 0 else "AVISO"
        report.add("Consistência", f"ANO_CMPT entre {ANO_MINIMO} e {ANO_MAXIMO}", status,
                   f"{fora_faixa:,} registros com ano fora do esperado", fora_faixa)

    # --- idades ---
    if "IDADE" in df.columns:
        idades = pd.to_numeric(df["IDADE"], errors="coerce")
        negativas = (idades < 0).sum()
        absurdas   = (idades > IDADE_MAXIMA_PLAUSIVEL).sum()
        status_neg = "OK" if negativas == 0 else "ERRO"
        status_abs = "OK" if absurdas  == 0 else "AVISO"
        report.add("Consistência", "Idades negativas", status_neg,
                   f"{negativas:,} registros com IDADE < 0", negativas)
        report.add("Consistência", f"Idades > {IDADE_MAXIMA_PLAUSIVEL}", status_abs,
                   f"{absurdas:,} registros com idade improvável", absurdas)

    # --- tempo de permanência ---
    if "DIAS_PERM" in df.columns:
        dias = pd.to_numeric(df["DIAS_PERM"], errors="coerce")
        negativos = (dias < 0).sum()
        absurdos   = (dias > DIAS_PERM_MAXIMOS_PLAUSIVEL).sum()
        status_neg = "OK" if negativos == 0 else "ERRO"
        status_abs = "OK" if absurdos  == 0 else "AVISO"
        report.add("Consistência", "Dias de permanência negativos", status_neg,
                   f"{negativos:,} registros com DIAS_PERM < 0", negativos)
        report.add("Consistência", f"Dias de permanência > {DIAS_PERM_MAXIMOS_PLAUSIVEL}", status_abs,
                   f"{absurdos:,} registros com internação muito longa", absurdos)

    # --- sexo ---
    if "SEXO" in df.columns:
        valores_sexo = df["SEXO"].dropna().astype(str).str.strip()
        validos = {"1", "3", "0"}
        invalidos = (~valores_sexo.isin(validos)).sum()
        status = "OK" if invalidos == 0 else "AVISO"
        report.add("Domínio", "Valores válidos em SEXO (0/1/3)", status,
                   f"{invalidos:,} registros com código de sexo desconhecido", invalidos)

    # --- mortes ---
    if "MORTE" in df.columns:
        mortes = pd.to_numeric(df["MORTE"], errors="coerce").fillna(0)
        taxa = mortes.sum() / n if n > 0 else 0
        status = "OK" if taxa <= LIMIAR_LETIDADE_MAXIMA else "AVISO"
        report.add("Negócio", "Taxa de mortalidade geral", status,
                   f"{taxa:.1%} dos registros resultaram em óbito", round(taxa, 4))

    logger.info("Validação bruta concluída. %s", report.resumo())
    return report


# ---------------------------------------------------------------------------
# 2. VALIDAÇÃO DO MODELO DIMENSIONAL
# ---------------------------------------------------------------------------

def validate_dimensional(conn: sqlite3.Connection) -> ValidationReport:
    """Valida a integridade referencial e regras de negócio no banco dimensional."""
    report = ValidationReport()
    logger.info("Iniciando validação do modelo dimensional...")

    def query(sql: str) -> int:
        return conn.execute(sql).fetchone()[0]

    # --- tabelas existem ---
    tabelas_esperadas = [
        "dim_tempo", "dim_local", "dim_paciente", "dim_diagnostico", "fato_internacoes"
    ]
    tabelas_existentes = {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    for tabela in tabelas_esperadas:
        existe = tabela in tabelas_existentes
        report.add("Estrutura", f"Tabela '{tabela}' existe",
                   "OK" if existe else "ERRO",
                   "Encontrada" if existe else "TABELA AUSENTE NO BANCO")

    if not all(t in tabelas_existentes for t in tabelas_esperadas):
        logger.error("Validação dimensional abortada: tabelas ausentes.")
        return report

    # --- contagem de linhas ---
    n_fato = query("SELECT COUNT(*) FROM fato_internacoes")
    report.add("Completude", "Linhas em fato_internacoes", "OK" if n_fato > 0 else "ERRO",
               f"{n_fato:,} registros", n_fato)

    for dim in ["dim_tempo", "dim_local", "dim_paciente", "dim_diagnostico"]:
        n = query(f"SELECT COUNT(*) FROM {dim}")
        report.add("Completude", f"Linhas em {dim}", "OK" if n > 1 else "AVISO",
                   f"{n:,} registros (inclui 1 desconhecido)", n)

    # --- registros "desconhecido" (sk=0) existem em todas as dimensões ---
    for dim, sk in [
        ("dim_tempo",       "sk_tempo"),
        ("dim_local",       "sk_local"),
        ("dim_paciente",    "sk_paciente"),
        ("dim_diagnostico", "sk_diag"),
    ]:
        existe_unk = query(f"SELECT COUNT(*) FROM {dim} WHERE {sk} = 0") > 0
        report.add("Integridade", f"Registro 'desconhecido' em {dim}",
                   "OK" if existe_unk else "ERRO",
                   "Presente (sk=0)" if existe_unk else "AUSENTE — FK nulas não terão destino")

    # --- FK nulas na tabela fato ---
    for fk in ["sk_tempo", "sk_local", "sk_paciente", "sk_diag"]:
        nulas = query(f"SELECT COUNT(*) FROM fato_internacoes WHERE {fk} IS NULL")
        status = "OK" if nulas == 0 else "ERRO"
        report.add("Integridade", f"FK nula: fato.{fk}", status,
                   f"{nulas:,} registros com FK nula", nulas)

    # --- proporção de FKs mapeadas para "desconhecido" ---
    if n_fato > 0:
        for fk, dim, sk in [
            ("sk_tempo",    "dim_tempo",       "sk_tempo"),
            ("sk_local",    "dim_local",       "sk_local"),
            ("sk_paciente", "dim_paciente",    "sk_paciente"),
            ("sk_diag",     "dim_diagnostico", "sk_diag"),
        ]:
            n_unk = query(
                f"SELECT COUNT(*) FROM fato_internacoes WHERE {fk} = "
                f"(SELECT {sk} FROM {dim} WHERE {sk} = 0)"
            )
            pct = n_unk / n_fato
            status = "OK" if pct <= LIMIAR_DESCONHECIDOS_FATO else "AVISO"
            report.add(
                "Qualidade", f"Fato.{fk} → desconhecido",
                status, f"{n_unk:,} ({pct:.1%}) mapeados para registro desconhecido",
                round(pct, 4),
            )

    # --- regras de negócio ---
    negativos_obitos = query(
        "SELECT COUNT(*) FROM fato_internacoes WHERE qt_obitos < 0"
    )
    report.add("Negócio", "qt_obitos >= 0",
               "OK" if negativos_obitos == 0 else "ERRO",
               f"{negativos_obitos:,} registros com óbitos negativos", negativos_obitos)

    obitos_maiores_internacoes = query(
        "SELECT COUNT(*) FROM fato_internacoes WHERE qt_obitos > qt_internacoes"
    )
    report.add("Negócio", "qt_obitos <= qt_internacoes",
               "OK" if obitos_maiores_internacoes == 0 else "ERRO",
               f"{obitos_maiores_internacoes:,} registros com mais óbitos que internações",
               obitos_maiores_internacoes)

    negativos_dias = query(
        "SELECT COUNT(*) FROM fato_internacoes WHERE dias_permanencia < 0"
    )
    report.add("Negócio", "dias_permanencia >= 0",
               "OK" if negativos_dias == 0 else "ERRO",
               f"{negativos_dias:,} registros com permanência negativa", negativos_dias)

    taxa_mortalidade = query(
        "SELECT CAST(SUM(qt_obitos) AS FLOAT) / SUM(qt_internacoes) FROM fato_internacoes"
    ) or 0
    status = "OK" if taxa_mortalidade <= LIMIAR_LETIDADE_MAXIMA else "AVISO"
    report.add("Negócio", "Taxa de mortalidade geral (dimensional)", status,
               f"{taxa_mortalidade:.1%}", round(taxa_mortalidade, 4))

    logger.info("Validação dimensional concluída. %s", report.resumo())
    return report


# ---------------------------------------------------------------------------
# SALVAR RELATÓRIO
# ---------------------------------------------------------------------------

def salvar_relatorio(
    report_raw: ValidationReport,
    report_dim: ValidationReport,
    caminho: str = "data/relatorio_qualidade.csv",
) -> None:
    import os
    os.makedirs(os.path.dirname(caminho), exist_ok=True)

    df_raw = report_raw.to_dataframe()
    df_raw.insert(0, "escopo", "dado_bruto")

    df_dim = report_dim.to_dataframe()
    df_dim.insert(0, "escopo", "modelo_dimensional")

    df_final = pd.concat([df_raw, df_dim], ignore_index=True)
    df_final["data_validacao"] = pd.Timestamp.now().isoformat(timespec="seconds")

    df_final.to_csv(caminho, index=False, sep=";", encoding="utf-8-sig")
    logger.info("Relatório de qualidade salvo em: %s (%d checks)", caminho, len(df_final))


# ---------------------------------------------------------------------------
# MAIN (execução isolada para re-validar banco existente)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    db_path = sys.argv[1] if len(sys.argv) > 1 else "data/processed/sih_datasus.db"

    with sqlite3.connect(db_path) as conn:
        report_dim = validate_dimensional(conn)

    salvar_relatorio(ValidationReport(), report_dim, "data/relatorio_qualidade.csv")

    if report_dim.tem_erros:
        logger.error("Validação concluída com ERROS. Verifique o relatório.")
        sys.exit(1)
    elif report_dim.tem_avisos:
        logger.warning("Validação concluída com AVISOS.")
    else:
        logger.info("Validação concluída sem problemas.")
