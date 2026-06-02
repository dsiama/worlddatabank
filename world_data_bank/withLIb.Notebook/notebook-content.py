# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3175b382-e687-43bb-aba9-3e6461383d4d",
# META       "default_lakehouse_name": "worldDataBank_lh",
# META       "default_lakehouse_workspace_id": "e8c731fe-74bf-4606-8e17-47b318ffbf98",
# META       "known_lakehouses": [
# META         {
# META           "id": "3175b382-e687-43bb-aba9-3e6461383d4d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

### dados downloaded https://datacatalog.worldbank.org/search/dataset/0037712/world-development-indicators

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
caminho_wdi = "/lakehouse/default/Files/fromWDB/"
df_wdiS = pd.read_csv(f"{caminho_wdi}WDISeries.csv")
df_wdiC = pd.read_csv(f"{caminho_wdi}WDICountry.csv")
df_wdiCS = pd.read_csv(f"{caminho_wdi}WDIcountry-series.csv")
df_wdiF = pd.read_csv(f"{caminho_wdi}WDIfootnote.csv")
df_wdiST = pd.read_csv(f"{caminho_wdi}WDIseries-time.csv")
df_wdiData = pd.read_csv(f"{caminho_wdi}WDICSV.csv")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_wdi

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

# O caminho do ficheiro na tua Lakehouse (o Fabric preenche isto por ti)
caminho_wdi = "/lakehouse/default/Files/fromWDB/WDISeries.csv"

# Ler o ficheiro gigante
print("A carregar base de dados global...")
df_wdi = pd.read_csv(caminho_wdi)

# Filtrar apenas para os indicadores de distribuição de riqueza (Decis/Quintis)
# Usamos o método str.contains para apanhar todos os que começam por 'SI.DST'
#df_riqueza = df_wdi[df_wdi['Indicator Code'].str.contains('SI.DST', na=False)]

print("Feito! Tabela pronta a usar.")
#display(df_riqueza.head())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import wbgapi as wb
import pandas as pd

def extrair_distribuicao_riqueza_global():
    """
    Vai à API do Banco Mundial e extrai os indicadores de distribuição 
    de riqueza (Decis extremos e Quintis) para todos os países 
    e para todos os anos historicamente disponíveis.
    """
    
    # 1. Definir os códigos oficiais
    indicadores = {
        'SI.DST.FRST.10': 'Lowest 10% (Mais pobres)',
        'SI.DST.FRST.20': 'Lowest 20% (1º Quintil)',
        'SI.DST.02ND.20': 'Second 20% (2º Quintil)',
        'SI.DST.03RD.20': 'Third 20% (3º Quintil)',
        'SI.DST.04TH.20': 'Fourth 20% (4º Quintil)',
        'SI.DST.05TH.20': 'Highest 20% (5º Quintil)',
        'SI.DST.10TH.10': 'Highest 10% (Mais ricos)'
    }
    
    print("A estabelecer ligação aos servidores do Banco Mundial...")
    print("A descarregar dados históricos para todos os países. Isto pode demorar alguns segundos...")
    
    # 2. Fazer o pedido à API
    # 'all' no 2º argumento puxa todos os países
    # time='all' puxa desde 1960 até ao ano atual
    df = wb.data.DataFrame(indicadores.keys(), 
                           'all', 
                           time='all', 
                           labels=True)
    
    # 3. Limpeza dos dados
    # Remover anos inteiros que não tenham qualquer dado registado para nenhum país
    df = df.dropna(axis=1, how='all')
    
    # Organizar as colunas e traduzir os códigos para nomes legíveis
    df = df.reset_index()
    df['Series'] = df['Series'].map(indicadores)
    
    # Renomear as colunas de identificação para um formato mais padronizado
    df = df.rename(columns={'economy': 'Country Code', 'Country': 'Country Name'})
    
    print(f"Extração concluída! Tabela gerada com {df.shape[0]} linhas e {df.shape[1]} colunas.")
    
    return df



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
