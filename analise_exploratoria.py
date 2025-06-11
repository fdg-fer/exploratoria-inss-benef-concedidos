# %%
### importando csv
import pandas as pd
from datetime import datetime


df = pd.read_csv('conced_inss_04_2025.csv', sep=';', encoding='utf-8')
df.head(1)
# %%
### extraindo e criando novas colunas com regex 
df[['COD_IBGE_resid', 'UF_resid', 'Cidade_resid']] = df['Mun Resid'].str.extract(r'(\d+)-(\w{2})-(.+)')
df['Tipo_Despacho'] = df['Despacho'].apply(lambda x: 'JUD' if 'Judicial' in x else 'ADM')
ano_atual = datetime.now().year
df['Idade'] = pd.to_datetime(df['Dt Nascimento'], format='%d/%m/%Y')
df['Idade'] = ano_atual-df['Idade'].dt.year

# %%
#df['freq_relativa'] = round(((df['Qtd'])/df['Qtd'].sum())*100, 2)
#df.sort_values(by='freq_relativa',ascending=False).reset_index(drop=True)
#df['freq_acum'] = df['freq_relativa'].cumsum()
#df['adm_%'] = round((df['ADM']/df['Qtd'])*100,2)
#df['jud_%'] = round(df['JUD']/df['Qtd']*100,2)
#df
# %%
df['Especie'].unique()

# %%

# %%
import duckdb

pareto = duckdb.query("""
WITH total_geral AS (
    SELECT count(*) AS total_qtd FROM df
    
), 
agregados AS (   
    SELECT 
        Especie, 
        COUNT(*) as qtd,
        COUNT(CASE WHEN Tipo_Despacho = 'ADM' THEN 1 ELSE NULL END) as total_adm,
        COUNT(CASE WHEN Tipo_Despacho = 'JUD' THEN 1 ELSE NULL END) as total_jud
    FROM df
    GROUP BY Especie
),

calculos AS (
    SELECT 
        Especie,
        qtd,
        CONCAT(ROUND((qtd * 100.0)/(SELECT total_qtd FROM total_geral), 2),'%') as freq_rel,
        CONCAT(ROUND(SUM((qtd * 100.0)/(SELECT total_qtd FROM total_geral)) OVER (ORDER BY qtd DESC), 2), '%') as freq_acum,  
    FROM agregados

)

SELECT * FROM calculos
ORDER BY qtd desc

""").to_df()

pareto
dfi.export(df, 'tabela_resultado.png')
#%%

import matplotlib.pyplot as plt
import seaborn as sns

plt.figure(figsize=(6,4))
sns.barplot(pareto, y='Especie', x='adm_percent', color='green', label='ADM')
plt.show()

# %%

import duckdb

bpc_uf = duckdb.query("""
WITH total_geral AS (
    SELECT count(*) AS total_qtd FROM df
    WHERE Especie = 'Amp. Social Pessoa Portadora Deficiencia'
    
), 
agregados AS (   
    SELECT 
        UF_resid, 
        COUNT(*) as qtd,
        COUNT(CASE WHEN Tipo_Despacho = 'ADM' THEN 1 ELSE NULL END) as total_adm,
        COUNT(CASE WHEN Tipo_Despacho = 'JUD' THEN 1 ELSE NULL END) as total_jud,
        COUNT(CASE WHEN Sexo = 'Feminino' THEN 1 ELSE NULL END) AS total_femin,
        COUNT(CASE WHEN Sexo = 'Masculino' THEN 1 ELSE NULL END) AS total_masc
    FROM df
    WHERE Especie = 'Amp. Social Pessoa Portadora Deficiencia'
    GROUP BY UF_resid
),

calculos AS (
    SELECT 
        UF_resid,
        qtd,
        ROUND((qtd * 100.0)/(SELECT total_qtd FROM total_geral), 1) as freq_rel,
        SUM(ROUND((qtd * 100.0) /(SELECT total_qtd FROM total_geral), 1)) OVER (ORDER BY qtd DESC) as freq_acum,  
        total_adm,
        ROUND((total_adm * 100)/(qtd), 2) as adm_percent,
        total_jud,
        ROUND((total_jud * 100)/(qtd), 2) as jud_percent,
        total_femin,
        total_masc
    FROM agregados

)

SELECT * FROM calculos
ORDER BY qtd desc

""").to_df()

bpc_uf

# %%

bpc_uf.describe()

# %%
df_ibge = pd.read_csv('ibge_censo_2022.csv')

df_ibge.head(5)

#%%
df[['COD_IBGE_resid']].value_counts().sum()