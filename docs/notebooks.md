# Notebooks e Transformações

!!! note "Sequência de notebooks"
    Os notebooks no Databricks seguem a ordem abaixo:

| Ordem | Notebook | Descrição |
|--------|-----------|------------|
| 0️⃣ | `Conexao`    | Cria conexão com o Supabase |
| 1️⃣ | `Vinhos 001` | Criação do banco |
| 2️⃣ | `Vinhos 002` | Bronze → Silver |
| 3️⃣ | `Vinhos 003` | Enriquecimento e joins |
| 4️⃣ | `Vinhos 004` | Delta Lake + SCD Type 2 |


---

## Exemplo de comando Delta

```sql
MERGE INTO silver_vinhos AS tgt 
USING bronze_vinhos AS src
ON tgt.id = src.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```