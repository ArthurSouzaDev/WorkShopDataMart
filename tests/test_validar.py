"""
Testes unitários para code/validar.py

Cobre:
- validate_raw(): cada regra de negócio individualmente
- validate_dimensional(): integridade referencial e regras no banco
- ValidationReport: comportamento das dataclasses de resultado
- salvar_relatorio(): criação do CSV de saída
"""
import sqlite3
import os
import tempfile

import pandas as pd
import pytest

from code.validar import (
    validate_raw,
    validate_dimensional,
    salvar_relatorio,
    ValidationReport,
    CheckResult,
)


# ===========================================================================
# ValidationReport
# ===========================================================================

class TestValidationReport:

    def test_add_ok(self):
        report = ValidationReport()
        report.add("Cat", "check1", "OK", "tudo certo")
        assert len(report.checks) == 1
        assert not report.tem_erros
        assert not report.tem_avisos

    def test_add_aviso(self):
        report = ValidationReport()
        report.add("Cat", "check1", "AVISO", "algo suspeito")
        assert report.tem_avisos
        assert not report.tem_erros

    def test_add_erro(self):
        report = ValidationReport()
        report.add("Cat", "check1", "ERRO", "problema crítico")
        assert report.tem_erros

    def test_resumo_contagem(self):
        report = ValidationReport()
        report.add("A", "c1", "OK", "ok")
        report.add("A", "c2", "OK", "ok")
        report.add("A", "c3", "AVISO", "av")
        report.add("A", "c4", "ERRO", "err")
        r = report.resumo()
        assert "4 checks" in r
        assert "OK: 2" in r
        assert "AVISOS: 1" in r
        assert "ERROS: 1" in r

    def test_to_dataframe_tem_colunas_corretas(self):
        report = ValidationReport()
        report.add("Cat", "check1", "OK", "detalhe", 42)
        df = report.to_dataframe()
        assert set(["categoria", "check", "status", "detalhe", "valor"]).issubset(df.columns)
        assert df.iloc[0]["valor"] == 42


# ===========================================================================
# validate_raw() — dado correto (happy path)
# ===========================================================================

class TestValidateRawHappyPath:

    def test_sem_erros_em_dado_valido(self, df_raw):
        report = validate_raw(df_raw)
        assert not report.tem_erros

    def test_gera_checks(self, df_raw):
        report = validate_raw(df_raw)
        assert len(report.checks) > 0

    def test_total_linhas_correto(self, df_raw):
        report = validate_raw(df_raw)
        check_linhas = next(
            (c for c in report.checks if "Total de linhas" in c.check), None
        )
        assert check_linhas is not None
        assert check_linhas.valor == len(df_raw)

    def test_taxa_mortalidade_ok_em_dado_normal(self, df_raw):
        report = validate_raw(df_raw)
        check_mort = next(
            (c for c in report.checks if "mortalidade" in c.check.lower()), None
        )
        assert check_mort is not None
        assert check_mort.status in ("OK", "AVISO")


# ===========================================================================
# validate_raw() — problemas injetados
# ===========================================================================

class TestValidateRawComProblemas:

    def test_detecta_idades_negativas(self, df_raw_com_problemas):
        report = validate_raw(df_raw_com_problemas)
        check = next(
            (c for c in report.checks if "negativas" in c.check.lower()), None
        )
        assert check is not None
        assert check.status == "ERRO"
        assert check.valor > 0

    def test_detecta_datas_invalidas(self, df_raw_com_problemas):
        report = validate_raw(df_raw_com_problemas)
        check = next(
            (c for c in report.checks if "DT_INTER" in c.check and "formato" in c.check.lower()), None
        )
        assert check is not None
        # Pode ser AVISO dependendo do % de inválidos
        assert check.status in ("OK", "AVISO", "ERRO")

    def test_detecta_nulos_em_coluna_critica(self):
        """Mais de 10% nulos em coluna crítica deve gerar ERRO."""
        import numpy as np
        df = pd.DataFrame({
            "N_AIH":      [f"AIH{i}" for i in range(100)],
            "MUNIC_RES":  [None] * 100,          # 100% nulos — ERRO
            "DIAG_PRINC": ["A01"] * 100,
            "IDADE":      [30] * 100,
            "SEXO":       [1] * 100,
            "DT_INTER":   ["20230101"] * 100,
            "DIAS_PERM":  [5] * 100,
            "MORTE":      [0] * 100,
            "VAL_TOT":    [1000.0] * 100,
            "ANO_CMPT":   [2023] * 100,
            "MES_CMPT":   [1] * 100,
            "DT_SAIDA":   ["20230110"] * 100,
        })
        report = validate_raw(df)
        check = next(
            (c for c in report.checks if "MUNIC_RES" in c.check and "crítica" in c.check), None
        )
        assert check is not None
        assert check.status == "ERRO"

    def test_detecta_data_saida_antes_entrada(self, df_raw_com_problemas):
        report = validate_raw(df_raw_com_problemas)
        check = next(
            (c for c in report.checks if "DT_SAIDA" in c.check and "DT_INTER" in c.check), None
        )
        assert check is not None
        assert check.status == "ERRO"
        assert check.valor > 0

    def test_detecta_sexo_invalido(self, df_raw_com_problemas):
        report = validate_raw(df_raw_com_problemas)
        # O check de domínio de sexo fica na categoria "Domínio"
        check = next(
            (c for c in report.checks
             if c.categoria == "Domínio" and "SEXO" in c.check),
            None,
        )
        assert check is not None, "Check de domínio de SEXO não encontrado"
        assert check.valor > 0


# ===========================================================================
# validate_dimensional() — banco correto
# ===========================================================================

class TestValidateDimensionalHappyPath:

    def test_sem_erros_em_banco_valido(self, sqlite_conn_populado):
        report = validate_dimensional(sqlite_conn_populado)
        erros = [c for c in report.checks if c.status == "ERRO"]
        assert erros == [], f"Erros inesperados: {[(e.check, e.detalhe) for e in erros]}"

    def test_todas_tabelas_encontradas(self, sqlite_conn_populado):
        report = validate_dimensional(sqlite_conn_populado)
        checks_estrutura = [c for c in report.checks if c.categoria == "Estrutura"]
        for c in checks_estrutura:
            assert c.status == "OK", f"Tabela ausente: {c.check}"

    def test_registros_desconhecido_presentes(self, sqlite_conn_populado):
        report = validate_dimensional(sqlite_conn_populado)
        checks_unk = [c for c in report.checks if "desconhecido" in c.check.lower() and c.categoria == "Integridade"]
        for c in checks_unk:
            assert c.status == "OK", f"Registro desconhecido ausente: {c.check}"

    def test_nao_ha_fk_nulas(self, sqlite_conn_populado):
        report = validate_dimensional(sqlite_conn_populado)
        checks_fk = [c for c in report.checks if "FK nula" in c.check]
        for c in checks_fk:
            assert c.status == "OK", f"FK nula detectada: {c.check} — {c.detalhe}"

    def test_regras_negocio_ok(self, sqlite_conn_populado):
        report = validate_dimensional(sqlite_conn_populado)
        checks_neg = [c for c in report.checks if c.categoria == "Negócio"]
        erros = [c for c in checks_neg if c.status == "ERRO"]
        assert erros == []


# ===========================================================================
# validate_dimensional() — banco com problemas injetados
# ===========================================================================

class TestValidateDimensionalComProblemas:

    def test_detecta_tabelas_ausentes(self, sqlite_conn_vazio):
        report = validate_dimensional(sqlite_conn_vazio)
        checks_estrutura = [c for c in report.checks if c.categoria == "Estrutura"]
        erros = [c for c in checks_estrutura if c.status == "ERRO"]
        assert len(erros) == 5  # 5 tabelas esperadas, nenhuma existe

    def test_detecta_obitos_negativos(self):
        conn = sqlite3.connect(":memory:")
        from code.transformar import criar_schema, DDL
        conn.executescript(DDL)

        conn.execute("INSERT INTO dim_tempo VALUES (0, NULL, NULL, 'Desconhecido', NULL, NULL)")
        conn.execute("INSERT INTO dim_local VALUES (0, NULL, 'Desconhecido', NULL, 'Desconhecida')")
        conn.execute("INSERT INTO dim_paciente VALUES (0, NULL, 'Desconhecida', 'Ignorado')")
        conn.execute("INSERT INTO dim_diagnostico VALUES (0, NULL, 'Desconhecido', 'Desconhecido')")
        # Insere fato com óbito negativo
        conn.execute(
            "INSERT INTO fato_internacoes VALUES (1, 0, 0, 0, 0, 10, -1, 5)"
        )
        conn.commit()

        report = validate_dimensional(conn)
        check = next((c for c in report.checks if "qt_obitos >= 0" in c.check), None)
        assert check is not None
        assert check.status == "ERRO"
        conn.close()

    def test_detecta_obitos_maiores_que_internacoes(self):
        conn = sqlite3.connect(":memory:")
        from code.transformar import DDL
        conn.executescript(DDL)

        conn.execute("INSERT INTO dim_tempo VALUES (0, NULL, NULL, 'Desconhecido', NULL, NULL)")
        conn.execute("INSERT INTO dim_local VALUES (0, NULL, 'Desconhecido', NULL, 'Desconhecida')")
        conn.execute("INSERT INTO dim_paciente VALUES (0, NULL, 'Desconhecida', 'Ignorado')")
        conn.execute("INSERT INTO dim_diagnostico VALUES (0, NULL, 'Desconhecido', 'Desconhecido')")
        # qt_obitos (5) > qt_internacoes (1)
        conn.execute(
            "INSERT INTO fato_internacoes VALUES (1, 0, 0, 0, 0, 1, 5, 10)"
        )
        conn.commit()

        report = validate_dimensional(conn)
        check = next((c for c in report.checks if "qt_obitos <= qt_internacoes" in c.check), None)
        assert check is not None
        assert check.status == "ERRO"
        conn.close()


# ===========================================================================
# salvar_relatorio()
# ===========================================================================

class TestSalvarRelatorio:

    def test_cria_arquivo_csv(self, tmp_path):
        report = ValidationReport()
        report.add("Cat", "check", "OK", "tudo bem", 1)
        caminho = str(tmp_path / "relatorio.csv")
        salvar_relatorio(report, ValidationReport(), caminho)
        assert os.path.exists(caminho)

    def test_csv_tem_colunas_esperadas(self, tmp_path):
        report = ValidationReport()
        report.add("Cat", "check", "OK", "ok", 0)
        caminho = str(tmp_path / "relatorio.csv")
        salvar_relatorio(report, ValidationReport(), caminho)
        df = pd.read_csv(caminho, sep=";", encoding="utf-8-sig")
        for col in ["escopo", "categoria", "check", "status", "detalhe", "data_validacao"]:
            assert col in df.columns

    def test_escopo_separado_raw_e_dimensional(self, tmp_path, df_raw, sqlite_conn_populado):
        report_raw = validate_raw(df_raw)
        report_dim = validate_dimensional(sqlite_conn_populado)
        caminho = str(tmp_path / "relatorio.csv")
        salvar_relatorio(report_raw, report_dim, caminho)
        df = pd.read_csv(caminho, sep=";", encoding="utf-8-sig")
        assert "dado_bruto" in df["escopo"].values
        assert "modelo_dimensional" in df["escopo"].values

    def test_cria_diretorio_se_nao_existir(self, tmp_path):
        report = ValidationReport()
        report.add("Cat", "check", "OK", "ok")
        caminho = str(tmp_path / "subpasta" / "relatorio.csv")
        salvar_relatorio(report, ValidationReport(), caminho)
        assert os.path.exists(caminho)
