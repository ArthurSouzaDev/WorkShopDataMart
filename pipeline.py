#!/usr/bin/env python3
"""
Orquestrador do Pipeline ETL — Data Mart SIH/DATASUS

Uso:
    python pipeline.py                          # roda todas as etapas com config padrão
    python pipeline.py --estado PA --ano 2024   # parametriza via CLI
    python pipeline.py --etapa transformar      # roda só uma etapa específica
    python pipeline.py --etapa validar          # re-valida banco existente sem reprocessar

Etapas disponíveis: extrair | converter | limpar | transformar | validar | all
"""
import argparse
import logging
import os
import sys
import time

import yaml


# ---------------------------------------------------------------------------
# CONFIGURAÇÃO
# ---------------------------------------------------------------------------

def carregar_config(caminho: str = "config.yaml") -> dict:
    if not os.path.exists(caminho):
        print(f"[ERRO] Arquivo de configuração não encontrado: {caminho}", file=sys.stderr)
        sys.exit(1)
    with open(caminho, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def configurar_logging(config: dict) -> None:
    nivel_str = config.get("log", {}).get("nivel", "INFO").upper()
    nivel = getattr(logging, nivel_str, logging.INFO)
    arquivo_log = config.get("log", {}).get("arquivo", "logs/pipeline.log")

    os.makedirs(os.path.dirname(arquivo_log), exist_ok=True)

    logging.basicConfig(
        level=nivel,
        format="%(asctime)s [%(levelname)-8s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(arquivo_log, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


# ---------------------------------------------------------------------------
# ARGPARSE
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipeline ETL — Data Mart SIH/DATASUS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--estado",
        type=str,
        metavar="UF",
        help="Sigla do estado (ex: TO, PA, SP). Substitui o valor do config.yaml.",
    )
    parser.add_argument(
        "--ano",
        type=int,
        metavar="YYYY",
        help="Ano dos dados (ex: 2023). Substitui o valor do config.yaml.",
    )
    parser.add_argument(
        "--mes",
        type=int,
        metavar="M",
        choices=range(1, 13),
        help="Mês dos dados (1-12). Substitui o valor do config.yaml.",
    )
    parser.add_argument(
        "--etapa",
        choices=["all", "extrair", "converter", "limpar", "transformar", "validar"],
        default="all",
        help="Etapa a executar (padrão: all).",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        metavar="ARQUIVO",
        help="Caminho para o arquivo de configuração (padrão: config.yaml).",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# ETAPAS
# ---------------------------------------------------------------------------

def etapa_extrair(config: dict, logger: logging.Logger) -> None:
    from code.extrair import extrair_dados_datasus, exportar_para_sql

    estados = config["extracao"]["estados"]
    anos    = config["extracao"]["anos"]
    meses   = config["extracao"]["meses"]
    raw     = config["caminhos"]["_raw_resolvido"]

    df = extrair_dados_datasus(estados=estados, anos=anos, meses=meses, caminho_saida=raw)
    exportar_para_sql(df, db_path=config["caminhos"]["db"])


def etapa_converter(config: dict, logger: logging.Logger) -> None:
    from code.conversor import converter_sql_para_csv

    converter_sql_para_csv(
        caminho_sql=config["caminhos"]["dump_sql"],
        caminho_csv=config["caminhos"]["csv_bruto"],
    )


def etapa_limpar(config: dict, logger: logging.Logger) -> None:
    from code.limparDados import limpar_dados

    limpar_dados(
        caminho_entrada=config["caminhos"]["csv_bruto"],
        caminho_saida=config["caminhos"]["csv_limpo"],
    )


def etapa_transformar(config: dict, logger: logging.Logger) -> None:
    from code.transformar import main as transformar_main

    transformar_main(
        raw_file=config["caminhos"]["_raw_resolvido"],
        db_path=config["caminhos"]["db"],
        dump_path=config["caminhos"]["dump_sql"],
        municipios_csv=config["caminhos"]["municipios"],
        csv_pasta=config["caminhos"]["csv_dimensional"],
        relatorio_qualidade=config["caminhos"].get("relatorio_qualidade", "data/relatorio_qualidade.csv"),
    )


def etapa_validar(config: dict, logger: logging.Logger) -> None:
    """Re-valida o banco dimensional existente sem reprocessar os dados."""
    import sqlite3
    from code.validar import validate_dimensional, salvar_relatorio, ValidationReport

    db_path = config["caminhos"]["db"]
    relatorio = config["caminhos"].get("relatorio_qualidade", "data/relatorio_qualidade.csv")

    logger.info("Re-validando banco dimensional: %s", db_path)
    with sqlite3.connect(db_path) as conn:
        report_dim = validate_dimensional(conn)

    salvar_relatorio(ValidationReport(), report_dim, relatorio)
    logger.info("Relatório salvo em: %s", relatorio)

    if report_dim.tem_erros:
        raise RuntimeError(f"Validação dimensional falhou. Verifique: {relatorio}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

ORDEM_ETAPAS = ["extrair", "converter", "limpar", "transformar", "validar"]

DISPATCH = {
    "extrair":     etapa_extrair,
    "converter":   etapa_converter,
    "limpar":      etapa_limpar,
    "transformar": etapa_transformar,
    "validar":     etapa_validar,
}


def main() -> None:
    args = parse_args()
    config = carregar_config(args.config)

    # Sobrescrever config com argumentos CLI
    if args.estado:
        config["extracao"]["estados"] = [args.estado.upper()]
    if args.ano:
        config["extracao"]["anos"] = [args.ano]
    if args.mes:
        config["extracao"]["meses"] = [args.mes]

    configurar_logging(config)
    logger = logging.getLogger("pipeline")

    estado = config["extracao"]["estados"][0]
    ano    = config["extracao"]["anos"][0]
    mes    = config["extracao"]["meses"][0]

    raw_resolvido = config["caminhos"]["raw"].format(
        estado=estado.lower(), ano=ano, mes=mes
    )
    config["caminhos"]["_raw_resolvido"] = raw_resolvido

    logger.info("=" * 60)
    logger.info("  ETL Pipeline — Data Mart SIH/DATASUS")
    logger.info("=" * 60)
    logger.info("Estado: %s | Ano: %d | Mes: %02d", estado, ano, mes)
    logger.info("Arquivo raw: %s", raw_resolvido)
    logger.info("Banco: %s", config["caminhos"]["db"])
    logger.info("Log: %s", config["log"]["arquivo"])

    etapas_a_rodar = ORDEM_ETAPAS if args.etapa == "all" else [args.etapa]
    inicio_total = time.monotonic()
    resultados: dict[str, str] = {}

    for nome in etapas_a_rodar:
        logger.info("")
        logger.info("─" * 50)
        logger.info("Iniciando etapa: %s", nome.upper())
        t0 = time.monotonic()
        try:
            DISPATCH[nome](config, logger)
            duracao = time.monotonic() - t0
            resultados[nome] = f"OK ({duracao:.1f}s)"
            logger.info("Etapa %s concluida em %.1fs", nome.upper(), duracao)
        except Exception as exc:
            duracao = time.monotonic() - t0
            resultados[nome] = f"FALHA ({duracao:.1f}s)"
            logger.error("Etapa %s falhou apos %.1fs: %s", nome.upper(), duracao, exc, exc_info=True)
            logger.error("Pipeline interrompido.")
            _imprimir_resumo(resultados, etapas_a_rodar, logger)
            sys.exit(1)

    duracao_total = time.monotonic() - inicio_total
    _imprimir_resumo(resultados, etapas_a_rodar, logger)
    logger.info("Pipeline finalizado em %.1fs", duracao_total)


def _imprimir_resumo(resultados: dict, etapas: list, logger: logging.Logger) -> None:
    logger.info("")
    logger.info("=" * 50)
    logger.info("  RESUMO DA EXECUCAO")
    logger.info("=" * 50)
    for etapa in etapas:
        status = resultados.get(etapa, "NAO EXECUTADA")
        logger.info("  %-15s %s", etapa.upper(), status)
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
