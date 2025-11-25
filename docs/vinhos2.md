# Notebook – Vinhos 002

Dá continuidade à exploração dos dados, incluindo:

- Cálculo de valores totais
- Ranking das vendas
- Comparações entre cidades e produtos

Exemplo:

```python
df.groupby("cidade")["valor"].sum().sort_values(ascending=False)
```

Este notebook mostra como evoluir de uma análise simples para diagnósticos mais interessantes sobre o dataset criado.
