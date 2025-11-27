# Notebook – Vinhos 003

Gera um dataframe para cada tabela delta de bronze.

```python
df_tabela_vendas_vinhos_1   = spark.read.format("delta").table("bronze_vinhos.tabela_vendas_vinhos_1")
df_tabela_vendas_vinhos_2     = spark.read.format("delta").table("bronze_vinhos.tabela_vendas_vinhos_2")
df_tabela_vendas_vinhos_3   = spark.read.format("delta").table("bronze_vinhos.tabela_vendas_vinhos_3")
df_tabela_vendas_vinhos_4  = spark.read.format("delta").table("bronze_vinhos.tabela_vendas_vinhos_4")
df_tabela_vendas_vinhos_5    = spark.read.format("delta").table("bronze_vinhos.tabela_vendas_vinhos_5")
df_tabela_vendas_vinhos_6     = spark.read.format("delta").table("bronze_vinhos.tabela_vendas_vinhos_6")
df_tabela_vendas_vinhos_7    = spark.read.format("delta").table("bronze_vinhos.tabela_vendas_vinhos_7")
df_tabela_vendas_vinhos_8 = spark.read.format("delta").table("bronze_vinhos.tabela_vendas_vinhos_8")
df_tabela_vendas_vinhos_9    = spark.read.format("delta").table("bronze_vinhos.tabela_vendas_vinhos_9")
df_tabela_vendas_vinhos_10  = spark.read.format("delta").table("bronze_vinhos.tabela_vendas_vinhos_10")
```

Adiciona uma nova coluna (metadado) de data e hora de processamento e nome do arquivo de origem.

```python
from pyspark.sql.functions import current_timestamp, lit

df_tabela_vendas_vinhos_1   = df_tabela_vendas_vinhos_1.withColumn("data_hora_silver", current_timestamp()).withColumn("nome_tabela", lit("tabela_vendas_vinhos_1"))
df_tabela_vendas_vinhos_2     = df_tabela_vendas_vinhos_2.withColumn("data_hora_silver", current_timestamp()).withColumn("nome_tabela", lit("tabela_vendas_vinhos_2"))
df_tabela_vendas_vinhos_3   = df_tabela_vendas_vinhos_3.withColumn("data_hora_silver", current_timestamp()).withColumn("nome_tabela", lit("tabela_vendas_vinhos_3"))
df_tabela_vendas_vinhos_4  = df_tabela_vendas_vinhos_4.withColumn("data_hora_silver", current_timestamp()).withColumn("nome_tabela", lit("tabela_vendas_vinhos_4"))
df_tabela_vendas_vinhos_5    = df_tabela_vendas_vinhos_5.withColumn("data_hora_silver", current_timestamp()).withColumn("nome_tabela", lit("tabela_vendas_vinhos_5"))
df_tabela_vendas_vinhos_6     = df_tabela_vendas_vinhos_6.withColumn("data_hora_silver", current_timestamp()).withColumn("nome_tabela", lit("tabela_vendas_vinhos_6"))
df_tabela_vendas_vinhos_7    = df_tabela_vendas_vinhos_7.withColumn("data_hora_silver", current_timestamp()).withColumn("nome_tabela", lit("tabela_vendas_vinhos_7"))
df_tabela_vendas_vinhos_8 = df_tabela_vendas_vinhos_8.withColumn("data_hora_silver", current_timestamp()).withColumn("nome_tabela", lit("tabela_vendas_vinhos_8"))
df_tabela_vendas_vinhos_9    = df_tabela_vendas_vinhos_9.withColumn("data_hora_silver", current_timestamp()).withColumn("nome_tabela", lit("tabela_vendas_vinhos_9"))
df_tabela_vendas_vinhos_10  = df_tabela_vendas_vinhos_10.withColumn("data_hora_silver", current_timestamp()).withColumn("nome_tabela", lit("tabela_vendas_vinhos_10"))
```

Salva os dataframes em arquivos delta lake (formato de arquivo) no schema/database "bronze". As tabelas geradas são do tipo MANAGED (gerenciadas). Feito no Vinhos 002(anterior).

```python
df_tabela_vendas_vinhos_1.write.format('delta').mode("overwrite").saveAsTable("bronze.tabela_vendas_vinhos_1")
df_tabela_vendas_vinhos_2.write.format('delta').mode("overwrite").saveAsTable("bronze.tabela_vendas_vinhos_2")
df_tabela_vendas_vinhos_3.write.format('delta').mode("overwrite").saveAsTable("bronze.tabela_vendas_vinhos_3")
df_tabela_vendas_vinhos_4.write.format('delta').mode("overwrite").saveAsTable("bronze.tabela_vendas_vinhos_4")
df_tabela_vendas_vinhos_5.write.format('delta').mode("overwrite").saveAsTable("bronze.tabela_vendas_vinhos_5")
df_tabela_vendas_vinhos_6.write.format('delta').mode("overwrite").saveAsTable("bronze.tabela_vendas_vinhos_6")
df_tabela_vendas_vinhos_7.write.format('delta').mode("overwrite").saveAsTable("bronze.tabela_vendas_vinhos_7")
df_tabela_vendas_vinhos_8.write.format('delta').mode("overwrite").saveAsTable("bronze.tabela_vendas_vinhos_8")
df_tabela_vendas_vinhos_9.write.format('delta').mode("overwrite").saveAsTable("bronze.tabela_vendas_vinhos_9")
df_tabela_vendas_vinhos_10.write.format('delta').mode("overwrite").saveAsTable("bronze.tabela_vendas_vinhos_10")
```

Maiusculas, tirando siglas, etc e gravando no formato delta no Silver; Aplicando Data Quality;

```python
from pyspark.sql import functions as F

# ---------- Helpers ----------
def _apply_name_rules(colname: str) -> str:
    """Regras de renome: upper + prefixos 'CD_', 'VL_', etc."""
    n = colname.upper()
    n = n.replace("CD_", "CODIGO_")
    n = n.replace("VL_", "VALOR_")
    n = n.replace("DT_", "DATA_")
    n = n.replace("NM_", "NOME_")
    n = n.replace("DS_", "DESCRICAO_")
    n = n.replace("NR_", "NUMERO_")
    n = n.replace("_UF", "_UNIDADE_FEDERATIVA")
    return n

def _safe_drop(df, cols):
    """Dropa colunas somente se existirem."""
    existing = set(df.columns)
    to_drop = [c for c in cols if c in existing]
    return df.drop(*to_drop) if to_drop else df

# ---------- Core ----------
def renomear_colunas_managed(src_fqn: str, dest_fqn: str = None):
    """
    Lê uma managed table (Delta) do metastore, aplica regras de renome,
    ajusta colunas de auditoria e salva como **managed table** via saveAsTable.
    - src_fqn: 'schema.tabela' de origem (ex.: 'bronze.tabela_vendas_vinhos_1')
    - dest_fqn: 'schema.tabela' de destino; se None, sobrescreve a própria origem
    """
    dest_fqn = dest_fqn or src_fqn

    # Lê como TABELA (managed)
    df = spark.read.format("delta").table(src_fqn)

    # Renomeia todas as colunas de uma vez (evita conflito de rename em loop)
    new_cols = [_apply_name_rules(c) for c in df.columns]
    df = df.toDF(*new_cols)

    # Remove colunas antigas, se existirem
    df = _safe_drop(df, ["DATA_HORA_BRONZE", "NOME_ARQUIVO"])

    # Adiciona colunas de auditoria pedidas
    df = (df
          .withColumn("NOME_ARQUIVO_BRONZE", F.lit(src_fqn))     # origem rastreável
          .withColumn("DATA_ARQUIVO_SILVER", F.current_timestamp())
         )

    # Salva como **Managed Table** (sem LOCATION) — sobrescrevendo destino
    (df.write
       .format("delta")
       .mode("overwrite")
       .saveAsTable(dest_fqn))

    return dest_fqn
```
    
```python
renomear_colunas_managed("bronze_vinhos.tabela_vendas_vinhos_1",   "silver_vinhos.tabela_vendas_vinhos_1")
renomear_colunas_managed("bronze_vinhos.tabela_vendas_vinhos_2",     "silver_vinhos.tabela_vendas_vinhos_2")
renomear_colunas_managed("bronze_vinhos.tabela_vendas_vinhos_3",   "silver_vinhos.tabela_vendas_vinhos_3")
renomear_colunas_managed("bronze_vinhos.tabela_vendas_vinhos_4",  "silver_vinhos.tabela_vendas_vinhos_4")
renomear_colunas_managed("bronze_vinhos.tabela_vendas_vinhos_5",    "silver_vinhos.tabela_vendas_vinhos_5")
renomear_colunas_managed("bronze_vinhos.tabela_vendas_vinhos_6",     "silver_vinhos.tabela_vendas_vinhos_6")
renomear_colunas_managed("bronze_vinhos.tabela_vendas_vinhos_7",    "silver_vinhos.tabela_vendas_vinhos_7")
renomear_colunas_managed("bronze_vinhos.tabela_vendas_vinhos_8",  "silver_vinhos.tabela_vendas_vinhos_8")
renomear_colunas_managed("bronze_vinhos.tabela_vendas_vinhos_9",    "silver_vinhos.tabela_vendas_vinhos_9")
renomear_colunas_managed("bronze_vinhos.tabela_vendas_vinhos_10",  "silver_vinhos.tabela_vendas_vinhos_10")
```

Verifica os dados gravados no formato delta lake tipo MANAGED na camada bronze.

```sql
SHOW TABLES IN silver_vinhos
```

Mostra os detalhes de uma tabela delta lake.

```sql 
DESCRIBE DETAIL silver_vinhos.tabela_vendas_vinhos_1;
```

Mostra se a tabela é MANAGED Ou EXTERNAL.

```sql
DESCRIBE EXTENDED silver_vinhos.tabela_vendas_vinhos_1;
--DESCRIBE TABLE EXTENDED tabela_vendas_vinhos_1_bronze_vinhos;
```