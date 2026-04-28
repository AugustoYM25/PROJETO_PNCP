import pandas as pd
import pyodbc
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import MiniBatchKMeans
from sklearn.metrics import silhouette_score
import warnings

warnings.filterwarnings('ignore')

print("1. Amostra aleatória")
conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=DB_PNCP;Trusted_Connection=yes;"
conn = pyodbc.connect(conn_str)


query = """
    SELECT TOP 20000 objetoContrato 
    FROM gold.contratos_enriquecidos 
    WHERE id_perfil_ml NOT IN (16, 18, 9, 11, 17, 19, 20)
    ORDER BY NEWID()
"""
df_amostra = pd.read_sql(query, conn)
conn.close()

print("2. Vetorizando a amostra")
vectorizer = TfidfVectorizer(max_df=0.5, min_df=5, max_features=1500)
X_amostra = vectorizer.fit_transform(df_amostra['objetoContrato'])

print("3.  K-Means na amostra")

kmeans = MiniBatchKMeans(n_clusters=15, random_state=42, batch_size=1000)
labels = kmeans.fit_predict(X_amostra)

print("4. Calculando Score ")
score = silhouette_score(X_amostra, labels, sample_size=20000)


print(f" Silhouette Score: {score:.4f}")
