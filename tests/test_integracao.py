"""
Testes de integração — execução ponta a ponta do pipeline de transformação.

Roda transformar.main() sobre dados sintéticos em diretório temporário e
verifica o estado completo do banco, CSVs exportados e relatório de qualidade.
Não depende de rede, DATASUS ou arquivos externos.
"""
import os
import sqlite3
import tempfile

import pandas as pd
import pytest


# ===========================================================================
# Helpers
# ===========================================================================

def _rodar_pipeline_completo(parquet_path: str, tmp_path) -> dict:
    """Executa transformar.main() em diretório isolado e retorna os caminhos."""
    from code.transformar import main as transformar_main

    db_path      = str(tmp_path / "sih_test.db")
    dump_path    = str(tmp_path / "sih_test.sql")
    csv_pasta    = str(tmp_path / "csv")
    relatorio    = str(tmp_path / "relatorio.csv")
    municipios   = "code/municipios.csv"

    transformar_main(
        raw_file=parquet_path,
        db_path=db_path,
        dump_path=dump_path,
        municipios_csv=municipios,
        csv_pasta=csv_pasta,
        relatorio_qualidade=relatorio,
    )

    return {
        "db_path":   db_path,
        "dump_path": dump_path,
        "csv_pasta": csv_pasta,
        "relatorio": relatorio,
    }


# ===========================================================================
# Testes de integração
# ===========================================================================

class TestPipelineIntegracao:

    @pytest.fixture(scope="class")
    def pipeline_output(self, parquet_path, tmp_path_factory):
        tmp = tmp_path_factory.mktemp("pipeline_out")
        return _rodar_pipeline_completo(parquet_path, tmp)

    # --- banco gerado ---

    def test_banco_sqlite_criado(self, pipeline_output):
        assert os.path.exists(pipeline_output["db_path"])
        assert os.path.getsize(pipeline_output["db_path"]) > 0

    def test_todas_tabelas_existem(self, pipeline_output):
        tabelas_esperadas = {
            "dim_tempo", "dim_local", "dim_paciente",
            "dim_diagnostico", "fato_internacoes",
        }
        with sqlite3.connect(pipeline_output["db_path"]) as conn:
            tabelas = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
        assert tabelas_esperadas.issubset(tabelas)

    def test_fato_internacoes_tem_100_linhas(self, pipeline_output):
        """O df_raw sintético tem 100 linhas; o fato deve ter exatamente 100."""
        with sqlite3.connect(pipeline_output["db_path"]) as conn:
            n = conn.execute("SELECT COUNT(*) FROM fato_internacoes").fetchone()[0]
        assert n == 100

    def test_integridade_referencial_sk_tempo(self, pipeline_output):
        with sqlite3.connect(pipeline_output["db_path"]) as conn:
            orfaos = conn.execute("""
                SELECT COUNT(*) FROM fato_internacoes f
                WHERE NOT EXISTS (
                    SELECT 1 FROM dim_tempo d WHERE d.sk_tempo = f.sk_tempo
                )
            """).fetchone()[0]
        assert orfaos == 0

    def test_integridade_referencial_sk_local(self, pipeline_output):
        with sqlite3.connect(pipeline_output["db_path"]) as conn:
            orfaos = conn.execute("""
                SELECT COUNT(*) FROM fato_internacoes f
                WHERE NOT EXISTS (
                    SELECT 1 FROM dim_local d WHERE d.sk_local = f.sk_local
                )
            """).fetchone()[0]
        assert orfaos == 0

    def test_integridade_referencial_sk_paciente(self, pipeline_output):
        with sqlite3.connect(pipeline_output["db_path"]) as conn:
            orfaos = conn.execute("""
                SELECT COUNT(*) FROM fato_internacoes f
                WHERE NOT EXISTS (
                    SELECT 1 FROM dim_paciente d WHERE d.sk_paciente = f.sk_paciente
                )
            """).fetchone()[0]
        assert orfaos == 0

    def test_integridade_referencial_sk_diag(self, pipeline_output):
        with sqlite3.connect(pipeline_output["db_path"]) as conn:
            orfaos = conn.execute("""
                SELECT COUNT(*) FROM fato_internacoes f
                WHERE NOT EXISTS (
                    SELECT 1 FROM dim_diagnostico d WHERE d.sk_diag = f.sk_diag
                )
            """).fetchone()[0]
        assert orfaos == 0

    def test_nao_ha_obitos_negativos(self, pipeline_output):
        with sqlite3.connect(pipeline_output["db_path"]) as conn:
            n = conn.execute(
                "SELECT COUNT(*) FROM fato_internacoes WHERE qt_obitos < 0"
            ).fetchone()[0]
        assert n == 0

    def test_nao_ha_permanencia_negativa(self, pipeline_output):
        with sqlite3.connect(pipeline_output["db_path"]) as conn:
            n = conn.execute(
                "SELECT COUNT(*) FROM fato_internacoes WHERE dias_permanencia < 0"
            ).fetchone()[0]
        assert n == 0

    # --- CSVs exportados ---

    def test_csvs_criados(self, pipeline_output):
        pasta = pipeline_output["csv_pasta"]
        arquivos_esperados = [
            "dim_tempo.csv", "dim_local.csv", "dim_paciente.csv",
            "dim_diagnostico.csv", "fato_internacoes.csv",
        ]
        for arq in arquivos_esperados:
            caminho = os.path.join(pasta, arq)
            assert os.path.exists(caminho), f"CSV não encontrado: {arq}"
            assert os.path.getsize(caminho) > 0

    def test_csv_fato_tem_100_linhas(self, pipeline_output):
        caminho = os.path.join(pipeline_output["csv_pasta"], "fato_internacoes.csv")
        df = pd.read_csv(caminho, sep=";")
        assert len(df) == 100

    def test_csv_dim_local_tem_coluna_regiao(self, pipeline_output):
        caminho = os.path.join(pipeline_output["csv_pasta"], "dim_local.csv")
        df = pd.read_csv(caminho, sep=";")
        assert "regiao" in df.columns

    # --- dump SQL ---

    def test_dump_sql_criado(self, pipeline_output):
        assert os.path.exists(pipeline_output["dump_path"])
        assert os.path.getsize(pipeline_output["dump_path"]) > 0

    def test_dump_sql_tem_create_table(self, pipeline_output):
        with open(pipeline_output["dump_path"], "r", encoding="utf-8") as f:
            conteudo = f.read()
        assert "CREATE TABLE" in conteudo
        assert "fato_internacoes" in conteudo

    # --- relatório de qualidade ---

    def test_relatorio_qualidade_criado(self, pipeline_output):
        assert os.path.exists(pipeline_output["relatorio"])

    def test_relatorio_tem_checks_de_ambos_escopos(self, pipeline_output):
        df = pd.read_csv(pipeline_output["relatorio"], sep=";", encoding="utf-8-sig")
        assert "dado_bruto" in df["escopo"].values
        assert "modelo_dimensional" in df["escopo"].values

    def test_relatorio_nao_tem_erros_criticos(self, pipeline_output):
        df = pd.read_csv(pipeline_output["relatorio"], sep=";", encoding="utf-8-sig")
        erros = df[df["status"] == "ERRO"]
        assert len(erros) == 0, (
            f"Erros encontrados no relatório:\n{erros[['escopo','check','detalhe']].to_string()}"
        )

    # --- queries analíticas do README ---

    def test_query_internacoes_por_municipio(self, pipeline_output):
        """Replica a Query 1 do README — deve rodar sem erros e retornar linhas."""
        with sqlite3.connect(pipeline_output["db_path"]) as conn:
            df = pd.read_sql_query("""
                SELECT l.nome_municipio,
                       SUM(f.qt_internacoes) AS total_internacoes,
                       SUM(f.qt_obitos)      AS total_obitos
                FROM fato_internacoes f
                JOIN dim_local l ON f.sk_local = l.sk_local
                GROUP BY l.nome_municipio
                ORDER BY total_internacoes DESC
                LIMIT 10
            """, conn)
        assert len(df) > 0
        assert "total_internacoes" in df.columns

    def test_query_faixa_etaria_permanencia(self, pipeline_output):
        """Replica a Query 2 do README — média de dias por faixa etária."""
        with sqlite3.connect(pipeline_output["db_path"]) as conn:
            df = pd.read_sql_query("""
                SELECT p.faixa_etaria,
                       AVG(f.dias_permanencia) AS media_dias
                FROM fato_internacoes f
                JOIN dim_paciente p ON f.sk_paciente = p.sk_paciente
                GROUP BY p.faixa_etaria
                ORDER BY media_dias DESC
            """, conn)
        assert len(df) > 0
        assert df["media_dias"].notna().all()

    def test_query_evolucao_mensal(self, pipeline_output):
        """Replica a Query 5 do README — total de internações por mês."""
        with sqlite3.connect(pipeline_output["db_path"]) as conn:
            df = pd.read_sql_query("""
                SELECT t.mes, t.nome_mes, SUM(f.qt_internacoes) AS total_internacoes
                FROM fato_internacoes f
                JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
                GROUP BY t.mes, t.nome_mes
                ORDER BY t.mes
            """, conn)
        assert len(df) > 0
        assert "total_internacoes" in df.columns
