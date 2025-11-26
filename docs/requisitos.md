# Requisitos

### Requisitos de software

Para executar o projeto localmente, é necessário ter instalado:

* Python 3.10 ou superior
* pip (gerenciador de pacotes do Python)
* Jupyter Notebook ou JupyterLab
* Git (para clonar o repositório)

### Bibliotecas python necessárias

Instale as dependências executando:

```bash
pip install pandas faker numpy matplotlib jupyter pyspark
```

### Requisitos Databricks

Schemas:

* workspace.landing
* workspace.bronze_vinhos
* workspace.silver_vinhos
* workspace.gold_vinhos

Volume:

* workspace.landing.dados_vinhos

Runtime recomendado:

* Databricks Runtime 12.0+

### Requisitos para Ingestão JDBC (Supabase)

* Conta no Supabase
* Banco PostgreSQL
* Credenciais válidas
* SSL habilitado (sslmode=require)

