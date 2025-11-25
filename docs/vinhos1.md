# Notebook – Vinhos 001

Este notebook executa as primeiras análises sobre as tabelas, tais como:

- Contagem de registros
- Quantidade total vendida
- Análises simples por tipo de vinho

Exemplo de operação:

```python
df.groupby("vinho")["quantidade"].sum()
```

Objetivo:

> Familiarizar-se com operações analíticas básicas de dados tabulares.
