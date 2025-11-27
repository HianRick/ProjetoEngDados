# Notebook – Vinhos 002

Mostra todos os arquivos que estão dentro do Volume chamado "dados" que foi criado dentro do catálogo workspace, schema/database default.

```python
display(dbutils.fs.ls('/Volumes/workspace/landing/dados_vinhos'))
```

Gera um dataframe para cada arquivo que está no Volume "dados".

```python
caminho_landing = '/Volumes/workspace/landing/dados_vinhos'

df_tabela_vendas_vinhos_1   = spark.read.option("infeschema", "true").option("header", "true").csv(f"{caminho_landing}/tabela_1/*.csv")
df_tabela_vendas_vinhos_2   = spark.read.option("infeschema", "true").option("header", "true").csv(f"{caminho_landing}/tabela_2/*.csv")
df_tabela_vendas_vinhos_3   = spark.read.option("infeschema", "true").option("header", "true").csv(f"{caminho_landing}/tabela_3/*.csv")
df_tabela_vendas_vinhos_4   = spark.read.option("infeschema", "true").option("header", "true").csv(f"{caminho_landing}/tabela_4/*.csv")
df_tabela_vendas_vinhos_5   = spark.read.option("infeschema", "true").option("header", "true").csv(f"{caminho_landing}/tabela_5/*.csv")
df_tabela_vendas_vinhos_6   = spark.read.option("infeschema", "true").option("header", "true").csv(f"{caminho_landing}/tabela_6/*.csv")
df_tabela_vendas_vinhos_7   = spark.read.option("infeschema", "true").option("header", "true").csv(f"{caminho_landing}/tabela_7/*.csv")
df_tabela_vendas_vinhos_8   = spark.read.option("infeschema", "true").option("header", "true").csv(f"{caminho_landing}/tabela_8/*.csv")
df_tabela_vendas_vinhos_9   = spark.read.option("infeschema", "true").option("header", "true").csv(f"{caminho_landing}/tabela_9/*.csv")
df_tabela_vendas_vinhos_10   = spark.read.option("infeschema", "true").option("header", "true").csv(f"{caminho_landing}/tabela_10/*.csv")
```

Adiciona uma nova coluna (metadado) de data e hora de processamento e nome do arquivo de origem.

```python
from pyspark.sql.functions import current_timestamp, lit

df_tabela_vendas_vinhos_1   = df_tabela_vendas_vinhos_1.withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("tabela_vendas_vinhos_1.csv"))
df_tabela_vendas_vinhos_2   = df_tabela_vendas_vinhos_2.withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("tabela_vendas_vinhos_2.csv"))
df_tabela_vendas_vinhos_3   = df_tabela_vendas_vinhos_3.withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("tabela_vendas_vinhos_3.csv"))
df_tabela_vendas_vinhos_4   = df_tabela_vendas_vinhos_4.withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("tabela_vendas_vinhos_4.csv"))
df_tabela_vendas_vinhos_5   = df_tabela_vendas_vinhos_5.withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("tabela_vendas_vinhos_5.csv"))
df_tabela_vendas_vinhos_6   = df_tabela_vendas_vinhos_6.withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("tabela_vendas_vinhos_6.csv"))
df_tabela_vendas_vinhos_7   = df_tabela_vendas_vinhos_7.withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("tabela_vendas_vinhos_7.csv"))
df_tabela_vendas_vinhos_8   = df_tabela_vendas_vinhos_8.withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("tabela_vendas_vinhos_8.csv"))
df_tabela_vendas_vinhos_9   = df_tabela_vendas_vinhos_9.withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("tabela_vendas_vinhos_9.csv"))
df_tabela_vendas_vinhos_10   = df_tabela_vendas_vinhos_10.withColumn("data_hora_bronze", current_timestamp()).withColumn("nome_arquivo", lit("tabela_vendas_vinhos_10.csv"))
```

Salva os dataframes em arquivos delta lake (formato de arquivo) no schema/database "bronze". As tabelas geradas são do tipo MANAGED (gerenciadas).

```python
df_tabela_vendas_vinhos_1.write.format('delta').mode("overwrite").saveAsTable("bronze_vinhos.tabela_vendas_vinhos_1")
df_tabela_vendas_vinhos_2.write.format('delta').mode("overwrite").saveAsTable("bronze_vinhos.tabela_vendas_vinhos_2")
df_tabela_vendas_vinhos_3.write.format('delta').mode("overwrite").saveAsTable("bronze_vinhos.tabela_vendas_vinhos_3")
df_tabela_vendas_vinhos_4.write.format('delta').mode("overwrite").saveAsTable("bronze_vinhos.tabela_vendas_vinhos_4")
df_tabela_vendas_vinhos_5.write.format('delta').mode("overwrite").saveAsTable("bronze_vinhos.tabela_vendas_vinhos_5")
df_tabela_vendas_vinhos_6.write.format('delta').mode("overwrite").saveAsTable("bronze_vinhos.tabela_vendas_vinhos_6")
df_tabela_vendas_vinhos_7.write.format('delta').mode("overwrite").saveAsTable("bronze_vinhos.tabela_vendas_vinhos_7")
df_tabela_vendas_vinhos_8.write.format('delta').mode("overwrite").saveAsTable("bronze_vinhos.tabela_vendas_vinhos_8")
df_tabela_vendas_vinhos_9.write.format('delta').mode("overwrite").saveAsTable("bronze_vinhos.tabela_vendas_vinhos_9")
df_tabela_vendas_vinhos_10.write.format('delta').mode("overwrite").saveAsTable("bronze_vinhos.tabela_vendas_vinhos_10")
```

Verifica os dados gravados no formato delta lake tipo MANAGED na camada bronze.

```sql
SHOW TABLES IN bronze_vinhos
```

Mostra os detalhes de uma tabela delta lake.

```sql
DESCRIBE DETAIL bronze_vinhos.tabela_vendas_vinhos_1;
```

Mostra se a tabela é MANAGED Ou EXTERNAL.

```sql
DESCRIBE EXTENDED bronze_vinhos.tabela_vendas_vinhos_1;
```