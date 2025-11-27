# Notebook – Vinhos 001

```sql

CREATE SCHEMA IF NOT EXISTS workspace.landing
COMMENT 'Schema/Database para dados bronze (delta)';

CREATE VOLUME IF NOT EXISTS workspace.landing.dados_vinhos
COMMENT 'Volume para dados brutos criados no schema/database landing';

CREATE SCHEMA IF NOT EXISTS workspace.bronze_vinhos
COMMENT 'Schema/Database para dados bronze (delta)';

CREATE SCHEMA IF NOT EXISTS workspace.silver_vinhos
COMMENT 'Schema/Database para dados silver (delta)';

CREATE SCHEMA IF NOT EXISTS workspace.gold_vinhos
COMMENT 'Schema/Database para dados gold (delta) - modelagem dimensional';
```
