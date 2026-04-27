import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import urllib
import re
import unicodedata
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import MiniBatchKMeans
import warnings


warnings.filterwarnings('ignore')


def limpar_texto(texto):
    if not isinstance(texto, str): return ""
    
    
    texto = texto.lower() 
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
    
    
    texto = re.sub(r'[^\w\s]', '', texto) 
    texto = re.sub(r'\d+', '', texto) 
    texto = texto.replace('o', '').replace('a', '') 
    
   
    stopwords_licitacao = {
        'a', 'o', 'e', 'de', 'do', 'da', 'dos', 'das', 'para', 'com', 'por', 'em', 'no', 'na', 'nos', 'nas', 'um', 'uma', 'os', 'as', 'que', 'ao', 'aos', 'ou', 'se', 'pela', 'pelo',
        'aquisicao', 'contratacao', 'fornecimento', 'prestacao', 'servico', 'servicos', 'compra', 'contrato', 'empresa', 'especializada', 'execucao',
        'lei', 'nr', 'nº', 'objeto', 'termo', 'referencia', 'edital', 'processo', 'ata', 'registro', 'precos', 'preço', 'uasg', 'anexo', 'item', 'itens',
        'material', 'materiais', 'insumo', 'insumos', 'bem', 'bens', 'consumo', 'permanente', 'diversos', 'diversas', 'outros', 'outras', 'geral', 'gerais',
        'atender', 'necessidade', 'necessidades', 'visando', 'destinado', 'destinados', 'demanda', 'demandas', 'suprir', 'uso', 'utilizacao', 'unidade', 'secretaria', 'municipio', 'prefeitura'
    }
    
    palavras = texto.split()
   
    palavras_limpas = [p for p in palavras if p not in stopwords_licitacao and len(p) > 2]
    return " ".join(palavras_limpas)


print("Iniciando conexão ")
conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=DB_PNCP;Trusted_Connection=yes;"
conn_leitura = pyodbc.connect(conn_str)
params = urllib.parse.quote_plus(conn_str)
engine_escrita = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)


print("\nTreinamento ")
df_amostra = pd.read_sql("SELECT TOP 150000 objetoContrato FROM silver.contratos WHERE objetoContrato IS NOT NULL", conn_leitura)
df_amostra['texto_limpo'] = df_amostra['objetoContrato'].apply(limpar_texto)


vetorizador = TfidfVectorizer(max_features=1500, max_df=0.5, min_df=10)
matriz_amostra = vetorizador.fit_transform(df_amostra['texto_limpo'])


print("Criando 20 clusters")
modelo_kmeans = MiniBatchKMeans(n_clusters=20, random_state=42, batch_size=3000)
modelo_kmeans.fit(matriz_amostra)


print("\n Criação dos clusters de 100k")
query_total = "SELECT * FROM silver.contratos WHERE objetoContrato IS NOT NULL"

for chunk in pd.read_sql(query_total, conn_leitura, chunksize=100000):
   
    chunk['texto_limpo_ia'] = chunk['objetoContrato'].apply(limpar_texto)
    
    
    matriz_chunk = vetorizador.transform(chunk['texto_limpo_ia'])
    chunk['id_perfil_ml'] = modelo_kmeans.predict(matriz_chunk)
    
    
    chunk = chunk.drop(columns=['texto_limpo_ia'])
    chunk.to_sql('contratos_enriquecidos', schema='gold', con=engine_escrita, if_exists='append', index=False)
    print(f"Lote processado e salvo na gold.contratos_enriquecidos...")

print("\nClusters criados")
conn_leitura.close()
