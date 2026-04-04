import pandas as pd

# 1. Carregar os dados
# Usamos sep=';' pois é o padrão comum em dumps de SQL/CSV do DATASUS
df = pd.read_csv('dados_extraidos.csv', sep=';', low_memory=False)

# 2. Selecionar apenas as colunas essenciais para o BI de Saúde
colunas_foco = [
    'ANO_CMPT', 'MES_CMPT', 'DT_INTER', 'DT_SAIDA', 'NASC', 
    'SEXO', 'IDADE', 'DIAG_PRINC', 'DIAS_PERM', 'MORTE', 
    'VAL_TOT', 'MUNIC_RES', 'RACA_COR', 'CNES'
]
df_limpo = df[colunas_foco].copy()

# 3. Tratamento de Datas
# O DATASUS armazena datas como YYYYMMDD em formato numérico. Vamos converter:
def ajustar_data(coluna):
    return pd.to_datetime(coluna.astype(str), format='%Y%m%d', errors='coerce')

df_limpo['DT_INTER'] = ajustar_data(df_limpo['DT_INTER'])
df_limpo['DT_SAIDA'] = ajustar_data(df_limpo['DT_SAIDA'])
df_limpo['NASC'] = ajustar_data(df_limpo['NASC'])

# 4. Criar colunas amigáveis (Enriquecimento)
# Sexo: 1 = Masculino, 3 = Feminino
df_limpo['SEXO_DESC'] = df_limpo['SEXO'].map({1: 'Masculino', 3: 'Feminino'}).fillna('Não Informado')

# Óbito: Geralmente a coluna MORTE > 0 indica falecimento
df_limpo['INDICADOR_OBITO'] = df_limpo['MORTE'].apply(lambda x: 'Sim' if x > 0 else 'Não')

# Raça/Cor: Mapeamento padrão do IBGE/SUS
mapa_raca = {
    1: 'Branca', 2: 'Preta', 3: 'Parda', 
    4: 'Amarela', 5: 'Indígena', 99: 'Sem Informação'
}
df_limpo['RACA_DESC'] = df_limpo['RACA_COR'].map(mapa_raca).fillna('Sem Informação')

# 5. Cálculos Adicionais
# Calcular idade exata se necessário (caso a coluna IDADE tenha inconsistências)
df_limpo['IDADE_ANOS'] = (df_limpo['DT_INTER'] - df_limpo['NASC']).dt.days // 365

# 6. Salvar para o BI (CSV otimizado ou Excel)
df_limpo.to_csv('saude_bi_final.csv', index=False, sep=';', encoding='utf-8-sig')

print("Tratamento concluído! Colunas geradas:", df_limpo.columns.tolist())