"""
Testes unitários e de integração para code/transformar.py

Cobre:
- faixa_etaria(): mapeamento de idades para grupos etários
- categoria_cid(): classificação de códigos CID-10
- build_dim_tempo(): estrutura, registro desconhecido, trimestres
- build_dim_local(): parsing de IBGE, mapeamento UF/região
- build_dim_paciente(): combinações idade×sexo, grupos etários
- build_dim_diagnostico(): validação de formato CID-10
- build_fato(): chaves surrogate, métricas, registros desconhecidos
"""
import pytest
import pandas as pd

from code.transformar import (
    faixa_etaria,
    categoria_cid,
    build_dim_tempo,
    build_dim_local,
    build_dim_paciente,
    build_dim_diagnostico,
    build_fato,
)


# ===========================================================================
# faixa_etaria()
# ===========================================================================

class TestFaixaEtaria:

    @pytest.mark.parametrize("idade,esperado", [
        (0,   "Lactância"),
        (1,   "Lactância"),
        (2,   "Primeira infância"),
        (4,   "Primeira infância"),
        (5,   "Segunda infância"),
        (10,  "Segunda infância"),
        (11,  "Pré-adolescência"),
        (13,  "Pré-adolescência"),
        (14,  "Adolescência"),
        (18,  "Adolescência"),
        (19,  "Pós-adolescência"),
        (26,  "Pós-adolescência"),
        (27,  "Adultidade"),
        (40,  "Adultidade"),
        (41,  "Meia-idade"),
        (65,  "Meia-idade"),
        (66,  "Terceira idade"),
        (80,  "Terceira idade"),
        (81,  "Quarta idade"),
        (130, "Quarta idade"),
    ])
    def test_faixas_validas(self, idade, esperado):
        assert faixa_etaria(idade) == esperado

    def test_idade_none_retorna_desconhecida(self):
        assert faixa_etaria(None) == "Desconhecida"

    def test_idade_string_invalida_retorna_desconhecida(self):
        assert faixa_etaria("abc") == "Desconhecida"

    def test_idade_como_string_numerica_funciona(self):
        assert faixa_etaria("25") == "Pós-adolescência"

    def test_idade_float_funciona(self):
        assert faixa_etaria(35.7) == "Adultidade"


# ===========================================================================
# categoria_cid()
# ===========================================================================

class TestCategoriaCid:

    @pytest.mark.parametrize("cid,esperado", [
        ("A01",  "Doenças infecciosas e parasitárias"),
        ("B20",  "Doenças infecciosas e parasitárias"),
        ("C34",  "Neoplasias"),
        ("D50",  "Neoplasias / Doenças do sangue"),
        ("E11",  "Doenças endócrinas e metabólicas"),
        ("F32",  "Transtornos mentais e comportamentais"),
        ("I10",  "Doenças do aparelho circulatório"),
        ("J18",  "Doenças do aparelho respiratório"),
        ("K35",  "Doenças do aparelho digestivo"),
        ("O80",  "Gravidez, parto e puerpério"),
        ("Z00",  "Fatores que influenciam o estado de saúde"),
        ("S72",  "Lesões e causas externas"),
        ("T14",  "Lesões e causas externas"),
    ])
    def test_cids_validos(self, cid, esperado):
        assert categoria_cid(cid) == esperado

    def test_cid_minusculo_normalizado(self):
        assert categoria_cid("a01") == "Doenças infecciosas e parasitárias"

    def test_cid_curto_demais_retorna_desconhecido(self):
        assert categoria_cid("A1") == "Desconhecido"

    def test_cid_sem_numero_retorna_desconhecido(self):
        assert categoria_cid("ABC") == "Desconhecido"

    def test_cid_none_retorna_desconhecido(self):
        assert categoria_cid(None) == "Desconhecido"

    def test_cid_string_vazia_retorna_desconhecido(self):
        assert categoria_cid("") == "Desconhecido"

    def test_letra_sem_mapeamento_retorna_desconhecido(self):
        # 'U' não está no dicionário CID-10 padrão
        assert categoria_cid("U07") == "Desconhecido"


# ===========================================================================
# build_dim_tempo()
# ===========================================================================

class TestBuildDimTempo:

    def test_sk_zero_e_registro_desconhecido(self, df_raw):
        dim = build_dim_tempo(df_raw)
        desconhecido = dim[dim["sk_tempo"] == 0].iloc[0]
        assert pd.isna(desconhecido["ano"])
        assert pd.isna(desconhecido["mes"])
        assert desconhecido["nome_mes"] == "Desconhecido"

    def test_tem_chave_primaria_unica(self, df_raw):
        dim = build_dim_tempo(df_raw)
        assert dim["sk_tempo"].is_unique

    def test_trimestres_corretos(self, df_raw):
        dim = build_dim_tempo(df_raw).dropna(subset=["mes"])
        for _, row in dim.iterrows():
            mes = int(row["mes"])
            esperado = (mes - 1) // 3 + 1
            assert row["trimestre"] == esperado, f"Mês {mes}: esperado Q{esperado}, obtido Q{row['trimestre']}"

    def test_nome_mes_janeiro(self, df_raw):
        dim = build_dim_tempo(df_raw)
        janeiro = dim[dim["mes"] == 1]
        assert not janeiro.empty
        assert (janeiro["nome_mes"] == "Janeiro").all()

    def test_ignora_datas_com_formato_invalido(self):
        # Strings que não são 8 dígitos devem ser descartadas
        df_invalido = pd.DataFrame({"DT_INTER": ["INVALIDA", "2023-01-15", "2023115", None]})
        dim = build_dim_tempo(df_invalido)
        # Só o registro desconhecido deve existir (nenhuma data válida)
        assert len(dim) == 1
        assert dim.iloc[0]["sk_tempo"] == 0

    def test_nao_duplica_mesma_data(self, df_raw):
        # df_raw tem DT_INTER repetidas — dim deve ter cada data uma única vez
        dim = build_dim_tempo(df_raw)
        datas_validas = dim.dropna(subset=["ano", "mes", "dia"])
        assert datas_validas[["ano", "mes", "dia"]].duplicated().sum() == 0


# ===========================================================================
# build_dim_local()
# ===========================================================================

class TestBuildDimLocal:

    def test_sk_zero_e_registro_desconhecido(self, df_raw, mapa_municipios):
        dim = build_dim_local(df_raw, mapa_municipios)
        desconhecido = dim[dim["sk_local"] == 0].iloc[0]
        assert pd.isna(desconhecido["cod_ibge"])
        assert desconhecido["nome_municipio"] == "Desconhecido"

    def test_tem_chave_primaria_unica(self, df_raw, mapa_municipios):
        dim = build_dim_local(df_raw, mapa_municipios)
        assert dim["sk_local"].is_unique

    def test_resolucao_de_uf_para_tocantins(self, mapa_municipios):
        df = pd.DataFrame({"MUNIC_RES": ["172100"]})  # Palmas-TO (prefixo 17)
        dim = build_dim_local(df, mapa_municipios)
        palmas = dim[dim["cod_ibge"] == "172100"]
        assert not palmas.empty
        assert palmas.iloc[0]["uf"] == "TO"
        assert palmas.iloc[0]["regiao"] == "Norte"

    def test_nome_municipio_resolvido_pelo_mapa(self, mapa_municipios):
        df = pd.DataFrame({"MUNIC_RES": ["172100"]})
        dim = build_dim_local(df, mapa_municipios)
        palmas = dim[dim["cod_ibge"] == "172100"]
        assert palmas.iloc[0]["nome_municipio"] == "Palmas"

    def test_codigo_ibge_invalido_e_ignorado(self, mapa_municipios):
        df = pd.DataFrame({"MUNIC_RES": ["ABCDEF", "XY", None]})
        dim = build_dim_local(df, mapa_municipios)
        # Só o registro desconhecido
        assert len(dim) == 1

    def test_codigos_duplicados_nao_geram_duplicatas(self, mapa_municipios):
        df = pd.DataFrame({"MUNIC_RES": ["172100", "172100", "172100"]})
        dim = build_dim_local(df, mapa_municipios)
        palmas = dim[dim["cod_ibge"] == "172100"]
        assert len(palmas) == 1


# ===========================================================================
# build_dim_paciente()
# ===========================================================================

class TestBuildDimPaciente:

    def test_sk_zero_e_registro_desconhecido(self, df_raw):
        dim = build_dim_paciente(df_raw)
        desconhecido = dim[dim["sk_paciente"] == 0].iloc[0]
        assert pd.isna(desconhecido["idade"])
        assert desconhecido["sexo"] == "Ignorado"

    def test_tem_chave_primaria_unica(self, df_raw):
        dim = build_dim_paciente(df_raw)
        assert dim["sk_paciente"].is_unique

    def test_sexo_mapeado_corretamente(self):
        df = pd.DataFrame({"IDADE": [30, 25, 40], "SEXO": [1, 3, 0]})
        dim = build_dim_paciente(df)
        sexos = set(dim["sexo"].dropna())
        assert "Masculino" in sexos
        assert "Feminino" in sexos
        assert "Ignorado" in sexos

    def test_faixa_etaria_calculada(self):
        df = pd.DataFrame({"IDADE": [5, 20, 50], "SEXO": [1, 1, 1]})
        dim = build_dim_paciente(df)
        faixas = set(dim["faixa_etaria"].dropna())
        assert "Segunda infância" in faixas
        assert "Pós-adolescência" in faixas
        assert "Meia-idade" in faixas

    def test_idade_negativa_e_convertida_para_none(self):
        df = pd.DataFrame({"IDADE": [-5], "SEXO": [1]})
        dim = build_dim_paciente(df)
        # Só o registro desconhecido (sk=0) + 1 registro com idade None
        sem_sk = dim[dim["sk_paciente"] > 0]
        if not sem_sk.empty:
            assert pd.isna(sem_sk.iloc[0]["idade"])


# ===========================================================================
# build_dim_diagnostico()
# ===========================================================================

class TestBuildDimDiagnostico:

    def test_sk_zero_e_registro_desconhecido(self, df_raw):
        dim = build_dim_diagnostico(df_raw)
        desconhecido = dim[dim["sk_diag"] == 0].iloc[0]
        assert pd.isna(desconhecido["cid"])

    def test_tem_chave_primaria_unica(self, df_raw):
        dim = build_dim_diagnostico(df_raw)
        assert dim["sk_diag"].is_unique

    def test_cid_invalido_descartado(self):
        df = pd.DataFrame({"DIAG_PRINC": ["A01", "INVALIDO", "123", None, "B20"]})
        dim = build_dim_diagnostico(df)
        cids = set(dim["cid"].dropna())
        assert "A01" in cids
        assert "B20" in cids
        assert "INVALIDO" not in cids
        assert "123" not in cids

    def test_cid_normalizado_para_maiusculo(self):
        df = pd.DataFrame({"DIAG_PRINC": ["a01", "b20"]})
        dim = build_dim_diagnostico(df)
        cids = set(dim["cid"].dropna())
        assert "A01" in cids
        assert "B20" in cids

    def test_categoria_preenchida(self, df_raw):
        dim = build_dim_diagnostico(df_raw)
        validos = dim.dropna(subset=["cid"])
        assert validos["categoria_cid"].notna().all()
        assert (validos["categoria_cid"] != "").all()


# ===========================================================================
# build_fato()
# ===========================================================================

class TestBuildFato:

    def test_numero_de_linhas_igual_ao_raw(self, dims_e_fato, df_raw):
        assert len(dims_e_fato["fato"]) == len(df_raw)

    def test_nao_ha_fk_nulas(self, dims_e_fato):
        fato = dims_e_fato["fato"]
        for fk in ["sk_tempo", "sk_local", "sk_paciente", "sk_diag"]:
            assert fato[fk].isna().sum() == 0, f"FK nula encontrada em {fk}"

    def test_qt_internacoes_sempre_um(self, dims_e_fato):
        assert (dims_e_fato["fato"]["qt_internacoes"] == 1).all()

    def test_qt_obitos_nao_negativo(self, dims_e_fato):
        assert (dims_e_fato["fato"]["qt_obitos"] >= 0).all()

    def test_obitos_nao_excedem_internacoes(self, dims_e_fato):
        fato = dims_e_fato["fato"]
        assert (fato["qt_obitos"] <= fato["qt_internacoes"]).all()

    def test_dias_permanencia_nao_negativo(self, dims_e_fato):
        assert (dims_e_fato["fato"]["dias_permanencia"] >= 0).all()

    def test_sk_fato_sequencial_a_partir_de_1(self, dims_e_fato):
        fato = dims_e_fato["fato"]
        assert fato["sk_fato"].min() == 1
        assert fato["sk_fato"].is_unique

    def test_todas_fks_existem_nas_dimensoes(self, dims_e_fato):
        fato = dims_e_fato["fato"]
        checks = [
            (fato["sk_tempo"],    dims_e_fato["dim_tempo"]["sk_tempo"]),
            (fato["sk_local"],    dims_e_fato["dim_local"]["sk_local"]),
            (fato["sk_paciente"], dims_e_fato["dim_paciente"]["sk_paciente"]),
            (fato["sk_diag"],     dims_e_fato["dim_diagnostico"]["sk_diag"]),
        ]
        for fk_series, dim_sk_series in checks:
            valores_invalidos = ~fk_series.isin(dim_sk_series)
            assert valores_invalidos.sum() == 0, (
                f"FK {fk_series.name} tem valores que não existem na dimensão"
            )
