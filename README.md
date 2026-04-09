
# Data Mart - Internamentos Hospitalares (DATASUS)

## Sobre o Projeto
Este projeto consiste na construção de um **Data Mart dimensional completo**, partindo da extração de dados brutos até a entrega de *insights* analíticos em um *dashboard*. O tema escolhido foca na **Saúde Pública**, utilizando dados reais do Sistema de Informações Hospitalares (SIH) do **DATASUS** referentes ao estado do Tocantins (2023).

O objetivo principal é permitir a análise multidimensional das internações, analisando volumes, óbitos, tempo de permanência e perfis de diagnóstico/pacientes, auxiliando na compreensão do panorama da saúde pública e tomada de decisões.

---

## Arquitetura e Modelo Dimensional
A arquitetura do projeto baseia-se em um pipeline de ETL (Extração, Transformação e Carga) automatizado em Python. O modelo de dados escolhido foi o **Star Schema** (Esquema em Estrela), focado em otimizar a performance de consultas analíticas.

### Dicionário de Dados

* **`fato_internacoes`** (Tabela Fato): Armazena os eventos quantitativos.
    * *Métricas:* `qt_internacoes` (Qtd. de Internações), `qt_obitos` (Qtd. de Óbitos), `dias_permanencia` (Dias de internação).
    * *Chaves Estrangeiras:* `sk_tempo`, `sk_local`, `sk_paciente`, `sk_diag`.
* **`dim_tempo`** (Dimensão de Tempo):
    * *Atributos:* `sk_tempo`, `ano`, `mes`, `dia`, `nome_mes`, `trimestre`.
* **`dim_local`** (Dimensão Geográfica):
    * *Atributos:* `sk_local`, `cod_ibge`, `nome_municipio`, `uf`, `regiao`.
* **`dim_paciente`** (Dimensão do Paciente):
    * *Atributos:* `sk_paciente`, `idade`, `faixa_etaria`, `sexo`.
* **`dim_diagnostico`** (Dimensão Clínica):
    * *Atributos:* `sk_diag`, `cid`, `descricao_cid`, `categoria_cid`.

---

## Pré-requisitos e Como Executar

### 1. Instalar as dependências
Certifique-se de que o Python está instalado. Instale as bibliotecas necessárias executando o comando abaixo no terminal:
```bash
pip install pysus pandas fastparquet sqlalchemy
```

### 2. Executar o Pipeline (Pasta `code/`)
O pipeline de dados foi modularizado. Execute os scripts na seguinte ordem para reproduzir o projeto desde a extração (gerando o arquivo `.parquet` na pasta `raw/`) até a carga no banco dimensional final (`processed/`):

1.  **Extração:** Baixa os dados brutos em formato `.parquet` diretamente do FTP do DATASUS usando a biblioteca PySUS.
    ```bash
    python extrair.py
    ```
2.  **Conversão:** Extrai do formato bruto do banco provisório para um arquivo CSV transacional de trabalho.
    ```bash
    python conversor.py
    ```
3.  **Limpeza e Enriquecimento:** Trata dados nulos, padroniza datas e enriquece colunas (ex: tradução de códigos de raça e sexo).
    ```bash
    python limparDados.py
    ```
4.  **Transformação e Carga (Star Schema):** Constrói as tabelas de Dimensão e Fato, gera os CSVs finais e exporta o dump em SQL e o arquivo do banco (`sih_datasus.db` e `sih_datasus.sql`).
    ```bash
    python transformar.py
    ```

---

## 📊 Perguntas de Negócio e Queries SQL

Abaixo estão 5 perguntas de negócio fundamentais e as *queries* SQL utilizadas para extrair os *insights* do nosso Data Mart:

**1. Qual é o total de internações e óbitos por município?**
```sql
SELECT l.nome_municipio, SUM(f.qt_internacoes) AS total_internacoes, SUM(f.qt_obitos) AS total_obitos
FROM fato_internacoes f
JOIN dim_local l ON f.sk_local = l.sk_local
GROUP BY l.nome_municipio
ORDER BY total_internacoes DESC
LIMIT 10;
```

**2. Qual é a faixa etária com o maior tempo médio de permanência hospitalar?**
```sql
SELECT p.faixa_etaria, AVG(f.dias_permanencia) AS media_dias_permanencia
FROM fato_internacoes f
JOIN dim_paciente p ON f.sk_paciente = p.sk_paciente
GROUP BY p.faixa_etaria
ORDER BY media_dias_permanencia DESC;
```

**3. Quais são as 5 principais categorias de diagnóstico (CID) que resultam em internação?**
```sql
SELECT d.categoria_cid, SUM(f.qt_internacoes) AS total_internacoes
FROM fato_internacoes f
JOIN dim_diagnostico d ON f.sk_diag = d.sk_diag
GROUP BY d.categoria_cid
ORDER BY total_internacoes DESC
LIMIT 5;
```

**4. Qual é a taxa de letalidade hospitalar agrupada por sexo?**
```sql
SELECT p.sexo, 
       SUM(f.qt_obitos) AS total_obitos, 
       SUM(f.qt_internacoes) AS total_internacoes,
       ROUND((CAST(SUM(f.qt_obitos) AS FLOAT) / SUM(f.qt_internacoes)) * 100, 2) AS taxa_letalidade_perc
FROM fato_internacoes f
JOIN dim_paciente p ON f.sk_paciente = p.sk_paciente
WHERE p.sexo != 'Ignorado'
GROUP BY p.sexo;
```

**5. Qual é a evolução mensal da quantidade de internações ao longo do ano analisado?**
```sql
SELECT t.mes, t.nome_mes, SUM(f.qt_internacoes) AS total_internacoes
FROM fato_internacoes f
JOIN dim_tempo t ON f.sk_tempo = t.sk_tempo
GROUP BY t.mes, t.nome_mes
ORDER BY t.mes;
```

---

## 📈 Dashboard e Visualizações
Para o consumo analítico e visualização interativa dos dados, utilizamos o **Power BI**. O *dashboard* consome as tabelas do nosso banco dimensional e responde às perguntas de negócio através das seguintes visualizações:


