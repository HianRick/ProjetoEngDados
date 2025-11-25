# Notebook – Conexão

O notebook **Conexao.ipynb** demonstra a etapa de leitura e importação dos CSVs gerados.

### Tecnologias:

- Python
- Pandas

### Funções realizadas

1. Carregamento dos dados usando `pandas.read_csv`
2. Visualização dos registros
3. Preparação para análise

Exemplo:

```python
import pandas as pd

df = pd.read_csv("tabelas/tabela_vendas_vinhos.csv")
df.head()
```
