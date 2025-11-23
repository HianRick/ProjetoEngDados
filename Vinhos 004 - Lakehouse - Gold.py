# Databricks notebook source
df_tabela_vendas_vinhos_1   = spark.read.format("delta").table("silver_vinhos.tabela_vendas_vinhos_1")
df_tabela_vendas_vinhos_2   = spark.read.format("delta").table("silver_vinhos.tabela_vendas_vinhos_2")
df_tabela_vendas_vinhos_3   = spark.read.format("delta").table("silver_vinhos.tabela_vendas_vinhos_3")
df_tabela_vendas_vinhos_4   = spark.read.format("delta").table("silver_vinhos.tabela_vendas_vinhos_4")
df_tabela_vendas_vinhos_5   = spark.read.format("delta").table("silver_vinhos.tabela_vendas_vinhos_5")
df_tabela_vendas_vinhos_6   = spark.read.format("delta").table("silver_vinhos.tabela_vendas_vinhos_6")
df_tabela_vendas_vinhos_7   = spark.read.format("delta").table("silver_vinhos.tabela_vendas_vinhos_7")
df_tabela_vendas_vinhos_8   = spark.read.format("delta").table("silver_vinhos.tabela_vendas_vinhos_8")
df_tabela_vendas_vinhos_9    = spark.read.format("delta").table("silver_vinhos.tabela_vendas_vinhos_9")
df_tabela_vendas_vinhos_10  = spark.read.format("delta").table("silver_vinhos.tabela_vendas_vinhos_10")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Adicionando metadados de data e hora de processamento e nome do arquivo de origem

# COMMAND ----------

# Unindo as tabelas usando unionByName
df_tabela_combinada = df_tabela_vendas_vinhos_1 \
    .unionByName(df_tabela_vendas_vinhos_2) \
    .unionByName(df_tabela_vendas_vinhos_3) \
    .unionByName(df_tabela_vendas_vinhos_4) \
    .unionByName(df_tabela_vendas_vinhos_5) \
    .unionByName(df_tabela_vendas_vinhos_6) \
    .unionByName(df_tabela_vendas_vinhos_7) \
    .unionByName(df_tabela_vendas_vinhos_8) \
    .unionByName(df_tabela_vendas_vinhos_9) \
    .unionByName(df_tabela_vendas_vinhos_10)

# Salvando a tabela combinada em uma nova tabela Delta, se necessário
df_tabela_combinada.write.format("delta").mode("overwrite").saveAsTable("silver_vinhos.tabela_vendas_vinhos_combinada")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_vinhos.tabela_vendas_vinhos_combinada

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM silver_vinhos.tabela_vendas_vinhos_combinada;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists gold_vinhos.dim_vendas_diarias;

# COMMAND ----------

df_tabela_vendas_vinhos_1.createOrReplaceTempView("tabela_vendas_vinhos_combinada")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gold_vinhos.dim_vendas_diarias
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT 
# MAGIC     YEAR(DATA_VENDA) AS ANO,                        
# MAGIC     MONTH(DATA_VENDA) AS MES,                       
# MAGIC     DAY(DATA_VENDA) AS DIA,                         
# MAGIC     STATUS_VENDA,                                  
# MAGIC     SUM(QUANTIDADE) AS QUANTIDADE_VENDIDA,        
# MAGIC     ROUND(SUM(CAST(REPLACE(PRECO, ',', '') AS DOUBLE)), 4) AS VALOR_VENDIDO 
# MAGIC FROM silver_vinhos.tabela_vendas_vinhos_combinada  
# MAGIC WHERE STATUS_VENDA IS NOT NULL  
# MAGIC GROUP BY 
# MAGIC     YEAR(DATA_VENDA),   
# MAGIC     MONTH(DATA_VENDA),  
# MAGIC     DAY(DATA_VENDA),    
# MAGIC     STATUS_VENDA      

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED gold_vinhos.dim_vendas_diarias

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH vendas_diarias_relacional AS (
# MAGIC     SELECT 
# MAGIC         YEAR(DATA_VENDA) AS ANO,                        
# MAGIC         MONTH(DATA_VENDA) AS MES,                       
# MAGIC         DAY(DATA_VENDA) AS DIA,                       
# MAGIC         STATUS_VENDA,                                 
# MAGIC         SUM(QUANTIDADE) AS QUANTIDADE_VENDIDA,          
# MAGIC         ROUND(SUM(CAST(REPLACE(PRECO, ',', '') AS DOUBLE)), 4) AS VALOR_VENDIDO 
# MAGIC     FROM silver_vinhos.tabela_vendas_vinhos_combinada  
# MAGIC     WHERE STATUS_VENDA IS NOT NULL  
# MAGIC     GROUP BY 
# MAGIC         YEAR(DATA_VENDA),    
# MAGIC         MONTH(DATA_VENDA),   
# MAGIC         DAY(DATA_VENDA),     
# MAGIC         STATUS_VENDA         
# MAGIC )
# MAGIC MERGE INTO gold_vinhos.dim_vendas_diarias AS dsv
# MAGIC USING vendas_diarias_relacional AS rsv
# MAGIC ON dsv.ANO = rsv.ANO AND dsv.MES = rsv.MES AND dsv.DIA = rsv.DIA AND dsv.STATUS_VENDA = rsv.STATUS_VENDA
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     dsv.QUANTIDADE_VENDIDA <> rsv.QUANTIDADE_VENDIDA OR
# MAGIC     dsv.VALOR_VENDIDO <> ROUND(rsv.VALOR_VENDIDO, 4) 
# MAGIC ) THEN
# MAGIC     UPDATE SET 
# MAGIC         dsv.QUANTIDADE_VENDIDA = rsv.QUANTIDADE_VENDIDA,
# MAGIC         dsv.VALOR_VENDIDO = ROUND(rsv.VALOR_VENDIDO, 4)  
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         ANO,
# MAGIC         MES,
# MAGIC         DIA,
# MAGIC         QUANTIDADE_VENDIDA,
# MAGIC         VALOR_VENDIDO,
# MAGIC         STATUS_VENDA
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         rsv.ANO,
# MAGIC         rsv.MES,
# MAGIC         rsv.DIA,
# MAGIC         rsv.QUANTIDADE_VENDIDA,
# MAGIC         ROUND(rsv.VALOR_VENDIDO, 4), 
# MAGIC         rsv.STATUS_VENDA
# MAGIC     );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from gold_vinhos.dim_vendas_diarias

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists gold_vinhos.dim_cliente

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gold_vinhos.dim_cliente
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT DISTINCT
# MAGIC     CLIENTE_ID,              
# MAGIC     CLIENTE_NOME,            
# MAGIC     CLIENTE_EMAIL            
# MAGIC FROM silver_vinhos.tabela_vendas_vinhos_combinada;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED gold_vinhos.dim_cliente

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cliente_relacional AS (
# MAGIC     SELECT DISTINCT
# MAGIC            CLIENTE_ID,
# MAGIC            CLIENTE_NOME,
# MAGIC            CLIENTE_EMAIL
# MAGIC       FROM silver_vinhos.tabela_vendas_vinhos_combinada 
# MAGIC )
# MAGIC MERGE INTO
# MAGIC     gold_vinhos.dim_cliente AS dc  
# MAGIC USING
# MAGIC     cliente_relacional AS rc 
# MAGIC ON rc.CLIENTE_ID = dc.CLIENTE_ID 
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     rc.CLIENTE_NOME <> dc.CLIENTE_NOME OR 
# MAGIC     rc.CLIENTE_EMAIL <> dc.CLIENTE_EMAIL
# MAGIC ) THEN
# MAGIC     UPDATE SET
# MAGIC         dc.CLIENTE_NOME = rc.CLIENTE_NOME,
# MAGIC         dc.CLIENTE_EMAIL = rc.CLIENTE_EMAIL
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (CLIENTE_ID, CLIENTE_NOME, CLIENTE_EMAIL)
# MAGIC     VALUES (rc.CLIENTE_ID, rc.CLIENTE_NOME, rc.CLIENTE_EMAIL);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_vinhos.dim_cliente

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists gold_vinhos.dim_localidade

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gold_vinhos.dim_localidade (
# MAGIC     SK_LOCALIDADE BIGINT GENERATED BY DEFAULT AS IDENTITY, 
# MAGIC     CIDADE   STRING,
# MAGIC     ESTADO   STRING,
# MAGIC     PAIS     STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED gold_vinhos.dim_localidade

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH localidade_relacional AS (
# MAGIC     SELECT DISTINCT
# MAGIC            CLIENTE_CIDADE,
# MAGIC            CLIENTE_ESTADO,
# MAGIC            PAIS_ORIGEM
# MAGIC       FROM silver_vinhos.tabela_vendas_vinhos_combinada  
# MAGIC )
# MAGIC MERGE INTO
# MAGIC     gold_vinhos.dim_localidade AS dl  
# MAGIC USING
# MAGIC     localidade_relacional AS rl 
# MAGIC ON rl.CLIENTE_CIDADE = dl.CIDADE AND rl.CLIENTE_ESTADO = dl.ESTADO AND rl.PAIS_ORIGEM = dl.PAIS
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     rl.CLIENTE_CIDADE <> dl.CIDADE OR
# MAGIC     rl.CLIENTE_ESTADO <> dl.ESTADO OR
# MAGIC     rl.PAIS_ORIGEM <> dl.PAIS
# MAGIC ) THEN
# MAGIC     UPDATE SET
# MAGIC         dl.CIDADE = rl.CLIENTE_CIDADE,
# MAGIC         dl.ESTADO = rl.CLIENTE_ESTADO,
# MAGIC         dl.PAIS = rl.PAIS_ORIGEM
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (CIDADE, ESTADO, PAIS)
# MAGIC     VALUES (rl.CLIENTE_CIDADE, rl.CLIENTE_ESTADO, rl.PAIS_ORIGEM);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_vinhos.dim_localidade

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists gold_vinhos.dim_dados_vinhos

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gold_vinhos.dim_dados_vinhos (
# MAGIC     SK_VINHO             BIGINT GENERATED BY DEFAULT AS IDENTITY,
# MAGIC     ID_VENDA             STRING,
# MAGIC     TIPO_VINHO           STRING,
# MAGIC     ROTULO               STRING,
# MAGIC     UVA                  STRING,
# MAGIC     TEOR_ALCOOLICO       STRING,
# MAGIC     PRECO                STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED gold_vinhos.dim_dados_vinhos

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH dados_vinhos_relacional AS (
# MAGIC     SELECT DISTINCT
# MAGIC            ID_VENDA,            
# MAGIC            PAIS_ORIGEM,
# MAGIC            TIPO_VINHO,
# MAGIC            ROTULO,
# MAGIC            UVA,
# MAGIC            TEOR_ALCOOLICO,
# MAGIC            PRECO      
# MAGIC       FROM silver_vinhos.tabela_vendas_vinhos_combinada 
# MAGIC )
# MAGIC MERGE INTO
# MAGIC     gold_vinhos.dim_dados_vinhos AS ddv
# MAGIC USING
# MAGIC     dados_vinhos_relacional AS rdv
# MAGIC ON ddv.ID_VENDA = rdv.ID_VENDA    
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     ddv.TIPO_VINHO <> rdv.TIPO_VINHO OR 
# MAGIC     ddv.ROTULO <> rdv.ROTULO OR
# MAGIC     ddv.UVA <> rdv.UVA OR
# MAGIC     ddv.TEOR_ALCOOLICO <> rdv.TEOR_ALCOOLICO OR
# MAGIC     ddv.PRECO <> rdv.PRECO
# MAGIC ) THEN
# MAGIC     UPDATE SET 
# MAGIC         ddv.TIPO_VINHO = rdv.TIPO_VINHO,
# MAGIC         ddv.ROTULO = rdv.ROTULO,
# MAGIC         ddv.UVA = rdv.UVA,
# MAGIC         ddv.TEOR_ALCOOLICO = rdv.TEOR_ALCOOLICO,
# MAGIC         ddv.PRECO = rdv.PRECO
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         ID_VENDA, TIPO_VINHO, ROTULO, UVA, TEOR_ALCOOLICO, PRECO
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         rdv.ID_VENDA, rdv.TIPO_VINHO, rdv.ROTULO, rdv.UVA, rdv.TEOR_ALCOOLICO, rdv.PRECO
# MAGIC     );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_vinhos.dim_dados_vinhos

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists gold_vinhos.fato_vendas_concluidas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gold_vinhos.fato_vendas_concluidas (
# MAGIC     ANO INT,                    
# MAGIC     MES INT,                   
# MAGIC     DIA INT,                    
# MAGIC     CIDADE STRING,              
# MAGIC     ESTADO STRING,              
# MAGIC     PAIS STRING,                
# MAGIC     TIPO_VINHO STRING,          
# MAGIC     CLIENTE_NOME STRING,        
# MAGIC     CLIENTE_EMAIL STRING,       
# MAGIC     CLIENTE_ID STRING,             
# MAGIC     QUANTIDADE_VENDIDA DOUBLE,     
# MAGIC     VALOR_VENDIDO DOUBLE 
# MAGIC
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED gold_vinhos.fato_vendas_concluidas

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH vendas_concluidas_relacional AS (
# MAGIC     SELECT 
# MAGIC         YEAR(v.DATA_VENDA) AS ANO,               
# MAGIC         MONTH(v.DATA_VENDA) AS MES,              
# MAGIC         DAY(v.DATA_VENDA) AS DIA,               
# MAGIC         l.CIDADE AS CIDADE,                
# MAGIC         l.ESTADO AS ESTADO,                
# MAGIC         l.PAIS AS PAIS,                         
# MAGIC         dv.TIPO_VINHO AS TIPO_VINHO,            
# MAGIC         c.CLIENTE_NOME AS CLIENTE_NOME,        
# MAGIC         c.CLIENTE_EMAIL AS CLIENTE_EMAIL,      
# MAGIC         c.CLIENTE_ID AS CLIENTE_ID,             
# MAGIC         SUM(v.QUANTIDADE) AS QUANTIDADE_VENDIDA, 
# MAGIC         ROUND(SUM(CAST(REPLACE(v.PRECO, ',', '') AS DOUBLE)), 4) AS VALOR_VENDIDO 
# MAGIC     FROM silver_vinhos.tabela_vendas_vinhos_combinada AS v
# MAGIC     JOIN gold_vinhos.dim_localidade AS l
# MAGIC         ON v.CLIENTE_CIDADE = l.CIDADE 
# MAGIC         AND v.CLIENTE_ESTADO = l.ESTADO 
# MAGIC         AND v.PAIS_ORIGEM = l.PAIS
# MAGIC     JOIN gold_vinhos.dim_dados_vinhos AS dv
# MAGIC         ON v.TIPO_VINHO = dv.TIPO_VINHO
# MAGIC     JOIN gold_vinhos.dim_cliente AS c
# MAGIC         ON v.CLIENTE_ID = c.CLIENTE_ID
# MAGIC     WHERE v.STATUS_VENDA = 'concluída'  
# MAGIC     GROUP BY 
# MAGIC         YEAR(v.DATA_VENDA), 
# MAGIC         MONTH(v.DATA_VENDA), 
# MAGIC         DAY(v.DATA_VENDA), 
# MAGIC         l.CIDADE, 
# MAGIC         l.ESTADO, 
# MAGIC         l.PAIS, 
# MAGIC         dv.TIPO_VINHO, 
# MAGIC         c.CLIENTE_NOME, 
# MAGIC         c.CLIENTE_EMAIL, 
# MAGIC         c.CLIENTE_ID
# MAGIC )
# MAGIC
# MAGIC MERGE INTO gold_vinhos.fato_vendas_concluidas AS fvc
# MAGIC USING vendas_concluidas_relacional AS rsv
# MAGIC ON fvc.ANO = rsv.ANO 
# MAGIC    AND fvc.MES = rsv.MES
# MAGIC    AND fvc.DIA = rsv.DIA
# MAGIC    AND fvc.TIPO_VINHO = rsv.TIPO_VINHO
# MAGIC    AND fvc.CIDADE = rsv.CIDADE
# MAGIC    AND fvc.ESTADO = rsv.ESTADO
# MAGIC    AND fvc.PAIS = rsv.PAIS
# MAGIC    AND fvc.CLIENTE_ID = rsv.CLIENTE_ID
# MAGIC    AND fvc.CLIENTE_NOME = rsv.CLIENTE_NOME
# MAGIC    AND fvc.CLIENTE_EMAIL = rsv.CLIENTE_EMAIL
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     fvc.QUANTIDADE_VENDIDA <> rsv.QUANTIDADE_VENDIDA OR
# MAGIC     fvc.VALOR_VENDIDO <> rsv.VALOR_VENDIDO
# MAGIC ) THEN
# MAGIC     UPDATE SET 
# MAGIC         fvc.QUANTIDADE_VENDIDA = rsv.QUANTIDADE_VENDIDA,
# MAGIC         fvc.VALOR_VENDIDO = rsv.VALOR_VENDIDO
# MAGIC
# MAGIC -- Quando não houver correspondência (inserir novos dados)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         ANO,
# MAGIC         MES,
# MAGIC         DIA,
# MAGIC         TIPO_VINHO,
# MAGIC         CIDADE,
# MAGIC         ESTADO,
# MAGIC         PAIS,
# MAGIC         CLIENTE_ID,
# MAGIC         CLIENTE_NOME,
# MAGIC         CLIENTE_EMAIL,
# MAGIC         QUANTIDADE_VENDIDA,
# MAGIC         VALOR_VENDIDO
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         rsv.ANO,
# MAGIC         rsv.MES,
# MAGIC         rsv.DIA,
# MAGIC         rsv.TIPO_VINHO,
# MAGIC         rsv.CIDADE,
# MAGIC         rsv.ESTADO,
# MAGIC         rsv.PAIS,
# MAGIC         rsv.CLIENTE_ID,
# MAGIC         rsv.CLIENTE_NOME,
# MAGIC         rsv.CLIENTE_EMAIL,
# MAGIC         rsv.QUANTIDADE_VENDIDA,
# MAGIC         rsv.VALOR_VENDIDO
# MAGIC     )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_vinhos.fato_vendas_concluidas
