# Notebook – Vinhos 004

Gerando um dataframe dos delta lake no container bronze do Azure Data Lake Storage.

```python
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
```

Unindo as tabelas usando unionByName.

```python
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
```

Salvando a tabela combinada em uma nova tabela Delta, se necessário.

```python
df_tabela_combinada.write.format("delta").mode("overwrite").saveAsTable("silver_vinhos.tabela_vendas_vinhos_combinada")
```

Adicionando metadados de data e hora de processamento e nome do arquivo de origem.

```sql
SELECT * FROM silver_vinhos.tabela_vendas_vinhos_combinada
```
```sql
SELECT COUNT(*) FROM silver_vinhos.tabela_vendas_vinhos_combinada; 
```

```sql 
DROP TABLE IF EXISTS gold_vinhos.dim_vendas_diarias;
```

```python
df_tabela_vendas_vinhos_1.createOrReplaceTempView("tabela_vendas_vinhos_combinada")
```

```sql
CREATE TABLE gold_vinhos.dim_vendas_diarias
USING DELTA
AS
SELECT 
    YEAR(DATA_VENDA) AS ANO,                        
    MONTH(DATA_VENDA) AS MES,                       
    DAY(DATA_VENDA) AS DIA,                         
    STATUS_VENDA,                                  
    SUM(QUANTIDADE) AS QUANTIDADE_VENDIDA,        
    ROUND(SUM(CAST(REPLACE(PRECO, ',', '') AS DOUBLE)), 4) AS VALOR_VENDIDO 
FROM silver_vinhos.tabela_vendas_vinhos_combinada  
WHERE STATUS_VENDA IS NOT NULL  
GROUP BY 
    YEAR(DATA_VENDA),   
    MONTH(DATA_VENDA),  
    DAY(DATA_VENDA),    
    STATUS_VENDA     
```

```sql
DESCRIBE TABLE EXTENDED gold_vinhos.dim_vendas_diarias
```

```sql
WITH vendas_diarias_relacional AS (
    SELECT 
        YEAR(DATA_VENDA) AS ANO,                        
        MONTH(DATA_VENDA) AS MES,                       
        DAY(DATA_VENDA) AS DIA,                       
        STATUS_VENDA,                                 
        SUM(QUANTIDADE) AS QUANTIDADE_VENDIDA,          
        ROUND(SUM(CAST(REPLACE(PRECO, ',', '') AS DOUBLE)), 4) AS VALOR_VENDIDO 
    FROM silver_vinhos.tabela_vendas_vinhos_combinada  
    WHERE STATUS_VENDA IS NOT NULL  
    GROUP BY 
        YEAR(DATA_VENDA),    
        MONTH(DATA_VENDA),   
        DAY(DATA_VENDA),     
        STATUS_VENDA         
)
MERGE INTO gold_vinhos.dim_vendas_diarias AS dsv
USING vendas_diarias_relacional AS rsv
ON dsv.ANO = rsv.ANO AND dsv.MES = rsv.MES AND dsv.DIA = rsv.DIA AND dsv.STATUS_VENDA = rsv.STATUS_VENDA

WHEN MATCHED AND (
    dsv.QUANTIDADE_VENDIDA <> rsv.QUANTIDADE_VENDIDA OR
    dsv.VALOR_VENDIDO <> ROUND(rsv.VALOR_VENDIDO, 4) 
) THEN
    UPDATE SET 
        dsv.QUANTIDADE_VENDIDA = rsv.QUANTIDADE_VENDIDA,
        dsv.VALOR_VENDIDO = ROUND(rsv.VALOR_VENDIDO, 4)  

WHEN NOT MATCHED THEN
    INSERT (
        ANO,
        MES,
        DIA,
        QUANTIDADE_VENDIDA,
        VALOR_VENDIDO,
        STATUS_VENDA
    )
    VALUES (
        rsv.ANO,
        rsv.MES,
        rsv.DIA,
        rsv.QUANTIDADE_VENDIDA,
        ROUND(rsv.VALOR_VENDIDO, 4), 
        rsv.STATUS_VENDA
    );
```

```sql 
SELECT * FROM gold_vinhos.dim_vendas_diarias
```

```sql
DROP TABLE IF EXISTS gold_vinhos.dim_cliente
```

```sql 
CREATE TABLE gold_vinhos.dim_cliente
USING DELTA
AS
SELECT DISTINCT
    CLIENTE_ID,              
    CLIENTE_NOME,            
    CLIENTE_EMAIL            
FROM silver_vinhos.tabela_vendas_vinhos_combinada;
```

```sql
DESCRIBE TABLE EXTENDED gold_vinhos.dim_cliente
```

```sql
WITH localidade_relacional AS (
    SELECT DISTINCT
           CLIENTE_CIDADE,
           CLIENTE_ESTADO,
           PAIS_ORIGEM
      FROM silver_vinhos.tabela_vendas_vinhos_combinada  
)
MERGE INTO
    gold_vinhos.dim_localidade AS dl  
USING
    localidade_relacional AS rl 
ON rl.CLIENTE_CIDADE = dl.CIDADE AND rl.CLIENTE_ESTADO = dl.ESTADO AND rl.PAIS_ORIGEM = dl.PAIS

WHEN MATCHED AND (
    rl.CLIENTE_CIDADE <> dl.CIDADE OR
    rl.CLIENTE_ESTADO <> dl.ESTADO OR
    rl.PAIS_ORIGEM <> dl.PAIS
) THEN
    UPDATE SET
        dl.CIDADE = rl.CLIENTE_CIDADE,
        dl.ESTADO = rl.CLIENTE_ESTADO,
        dl.PAIS = rl.PAIS_ORIGEM

WHEN NOT MATCHED THEN
    INSERT (CIDADE, ESTADO, PAIS)
    VALUES (rl.CLIENTE_CIDADE, rl.CLIENTE_ESTADO, rl.PAIS_ORIGEM);
```

```sql
select * from gold_vinhos.dim_localidade
```

```sql
drop table if exists gold_vinhos.dim_dados_vinhos
```

```sql
CREATE TABLE gold_vinhos.dim_dados_vinhos (
    SK_VINHO             BIGINT GENERATED BY DEFAULT AS IDENTITY,
    ID_VENDA             STRING,
    TIPO_VINHO           STRING,
    ROTULO               STRING,
    UVA                  STRING,
    TEOR_ALCOOLICO       STRING,
    PRECO                STRING
)
USING DELTA;
```

```sql
DESCRIBE TABLE EXTENDED gold_vinhos.dim_dados_vinhos
```

```sql
WITH dados_vinhos_relacional AS (
    SELECT DISTINCT
           ID_VENDA,            
           PAIS_ORIGEM,
           TIPO_VINHO,
           ROTULO,
           UVA,
           TEOR_ALCOOLICO,
           PRECO      
      FROM silver_vinhos.tabela_vendas_vinhos_combinada 
)
MERGE INTO
    gold_vinhos.dim_dados_vinhos AS ddv
USING
    dados_vinhos_relacional AS rdv
ON ddv.ID_VENDA = rdv.ID_VENDA    

WHEN MATCHED AND (
    ddv.TIPO_VINHO <> rdv.TIPO_VINHO OR 
    ddv.ROTULO <> rdv.ROTULO OR
    ddv.UVA <> rdv.UVA OR
    ddv.TEOR_ALCOOLICO <> rdv.TEOR_ALCOOLICO OR
    ddv.PRECO <> rdv.PRECO
) THEN
    UPDATE SET 
        ddv.TIPO_VINHO = rdv.TIPO_VINHO,
        ddv.ROTULO = rdv.ROTULO,
        ddv.UVA = rdv.UVA,
        ddv.TEOR_ALCOOLICO = rdv.TEOR_ALCOOLICO,
        ddv.PRECO = rdv.PRECO

WHEN NOT MATCHED THEN
    INSERT (
        ID_VENDA, TIPO_VINHO, ROTULO, UVA, TEOR_ALCOOLICO, PRECO
    )
    VALUES (
        rdv.ID_VENDA, rdv.TIPO_VINHO, rdv.ROTULO, rdv.UVA, rdv.TEOR_ALCOOLICO, rdv.PRECO
    );
```

```sql
SELECT * FROM gold_vinhos.dim_dados_vinhos
```

```sql
DROP TABLE IF EXISTS gold_vinhos.fato_vendas_concluidas
```

```sql
CREATE TABLE gold_vinhos.fato_vendas_concluidas (
    ANO INT,                    
    MES INT,                   
    DIA INT,                    
    CIDADE STRING,              
    ESTADO STRING,              
    PAIS STRING,                
    TIPO_VINHO STRING,          
    CLIENTE_NOME STRING,        
    CLIENTE_EMAIL STRING,       
    CLIENTE_ID STRING,             
    QUANTIDADE_VENDIDA DOUBLE,     
    VALOR_VENDIDO DOUBLE 

)
USING DELTA;
```

```sql
DESCRIBE TABLE EXTENDED gold_vinhos.fato_vendas_concluidas
```

```sql
%sql
WITH vendas_concluidas_relacional AS (
    SELECT 
        YEAR(v.DATA_VENDA) AS ANO,               
        MONTH(v.DATA_VENDA) AS MES,              
        DAY(v.DATA_VENDA) AS DIA,               
        l.CIDADE AS CIDADE,                
        l.ESTADO AS ESTADO,                
        l.PAIS AS PAIS,                         
        dv.TIPO_VINHO AS TIPO_VINHO,            
        c.CLIENTE_NOME AS CLIENTE_NOME,        
        c.CLIENTE_EMAIL AS CLIENTE_EMAIL,      
        c.CLIENTE_ID AS CLIENTE_ID,             
        SUM(v.QUANTIDADE) AS QUANTIDADE_VENDIDA, 
        ROUND(SUM(CAST(REPLACE(v.PRECO, ',', '') AS DOUBLE)), 4) AS VALOR_VENDIDO 
    FROM silver_vinhos.tabela_vendas_vinhos_combinada AS v
    JOIN gold_vinhos.dim_localidade AS l
        ON v.CLIENTE_CIDADE = l.CIDADE 
        AND v.CLIENTE_ESTADO = l.ESTADO 
        AND v.PAIS_ORIGEM = l.PAIS
    JOIN gold_vinhos.dim_dados_vinhos AS dv
        ON v.TIPO_VINHO = dv.TIPO_VINHO
    JOIN gold_vinhos.dim_cliente AS c
        ON v.CLIENTE_ID = c.CLIENTE_ID
    WHERE v.STATUS_VENDA = 'concluída'  
    GROUP BY 
        YEAR(v.DATA_VENDA), 
        MONTH(v.DATA_VENDA), 
        DAY(v.DATA_VENDA), 
        l.CIDADE, 
        l.ESTADO, 
        l.PAIS, 
        dv.TIPO_VINHO, 
        c.CLIENTE_NOME, 
        c.CLIENTE_EMAIL, 
        c.CLIENTE_ID
)

MERGE INTO gold_vinhos.fato_vendas_concluidas AS fvc
USING vendas_concluidas_relacional AS rsv
ON fvc.ANO = rsv.ANO 
   AND fvc.MES = rsv.MES
   AND fvc.DIA = rsv.DIA
   AND fvc.TIPO_VINHO = rsv.TIPO_VINHO
   AND fvc.CIDADE = rsv.CIDADE
   AND fvc.ESTADO = rsv.ESTADO
   AND fvc.PAIS = rsv.PAIS
   AND fvc.CLIENTE_ID = rsv.CLIENTE_ID
   AND fvc.CLIENTE_NOME = rsv.CLIENTE_NOME
   AND fvc.CLIENTE_EMAIL = rsv.CLIENTE_EMAIL

WHEN MATCHED AND (
    fvc.QUANTIDADE_VENDIDA <> rsv.QUANTIDADE_VENDIDA OR
    fvc.VALOR_VENDIDO <> rsv.VALOR_VENDIDO
) THEN
    UPDATE SET 
        fvc.QUANTIDADE_VENDIDA = rsv.QUANTIDADE_VENDIDA,
        fvc.VALOR_VENDIDO = rsv.VALOR_VENDIDO

-- Quando não houver correspondência (inserir novos dados)
WHEN NOT MATCHED THEN
    INSERT (
        ANO,
        MES,
        DIA,
        TIPO_VINHO,
        CIDADE,
        ESTADO,
        PAIS,
        CLIENTE_ID,
        CLIENTE_NOME,
        CLIENTE_EMAIL,
        QUANTIDADE_VENDIDA,
        VALOR_VENDIDO
    )
    VALUES (
        rsv.ANO,
        rsv.MES,
        rsv.DIA,
        rsv.TIPO_VINHO,
        rsv.CIDADE,
        rsv.ESTADO,
        rsv.PAIS,
        rsv.CLIENTE_ID,
        rsv.CLIENTE_NOME,
        rsv.CLIENTE_EMAIL,
        rsv.QUANTIDADE_VENDIDA,
        rsv.VALOR_VENDIDO
    )
```

```sql
SELECT * FROM gold_vinhos.fato_vendas_concluidas
```