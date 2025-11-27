# Notebook – Conexão

Conecta-se ao Supabase para persistir as tabelas e integra com o Databricks para armazenamento e processamento dos dados.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("supabase_to_landing").getOrCreate()

# Configuração JDBC do Supabase
jdbc_url = "jdbc:postgresql://aws-1-sa-east-1.pooler.supabase.com:6543/postgres?sslmode=require"

db_properties = {
    "user": "postgres.flrkrtvkykwlydijicwk",
    "password": "Projeto2025",
    "driver": "org.postgresql.Driver"
}

# Lista das tabelas com nome do Supabase
tabelas_supabase = [
    '"Tabela 1"',
    '"Tabela 2"',
    '"Tabela 3"',
    '"Tabela 4"',
    '"Tabela 5"',
    '"Tabela 6"',
    '"Tabela 7"',
    '"Tabela 8"',
    '"Tabela 9"',
    '"Tabela 10"'
]

# Caminho onde os CSV serão salvos DENTRO DO VOLUME
base_path = "/Volumes/workspace/landing/dados_vinhos/"

for tabela in tabelas_supabase:
    nome_limpo = tabela.replace('"', '').lower().replace(" ", "_")

    print(f"📥 Lendo tabela: {tabela}")

    df = spark.read.jdbc(url=jdbc_url, table=tabela, properties=db_properties)

    output_path = base_path + nome_limpo

    df.write.mode("overwrite").option("header", True).csv(output_path)

    print(f"✅ Salvou CSV em: {output_path}")

print("🎉 Processo finalizado!")
```

```python
df_check = spark.read.option("header", True).csv(output_path)
print(f"➡️ Linhas salvas em CSV de {tabela}: {df_check.count()}")
```