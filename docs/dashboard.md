Dashboard – Análise de Faturamento do Projeto Vinhos

Este dashboard foi desenvolvido no Databricks Lakeview para analisar as vendas de vinhos considerando diferentes perspectivas (ano, tipo de vinho, país e volume vendido).

Abaixo está a documentação completa das fontes de dados (datasets) e dos widgets que compõem a página de análise.


Exemplo de Dataset
fato_vendas_concluidas

```
SELECT * 
FROM workspace.gold_vinhos.fato_vendas_concluidas
```

Página Principal do Dashboard
Análise de Faturamento Anual de Vinhos

A página é composta por 6 widgets principais:

1. Gráfico de Barras
Vendas anuais por categoria

Utiliza o dataset GRAFICO_1.
Consultas usadas:

````
SELECT 
    ANO,
    TIPO_VINHO,
    SUM(VALOR_VENDIDO) AS VALOR_VENDIDO_TOTAL
 FROM gold_vinhos.fato_vendas_concluidas
 GROUP BY ANO, TIPO_VINHO
 ORDER BY ANO DESC, VALOR_VENDIDO_TOTAL DESC
```

Indicador (Counter)
Quantidade de vendas em 2025

Dataset: GRAFICO_2

```
    SELECT SUM(QUANTIDADE_VENDIDA) AS total_vendido_2025
    FROM workspace.gold_vinhos.fato_vendas_concluidas
    WHERE ANO = 2025;
```

 3. Indicador
Faturamento do Rosé – 2025

Dataset: GRAFICO_3

```
    SELECT SUM(VALOR_VENDIDO) AS total_vendido_2025
    FROM workspace.gold_vinhos.fato_vendas_concluidas
    WHERE ANO = 2025 AND TIPO_VINHO = 'Rosé';
```

4. Indicador
Faturamento do Espumante – 2025

Dataset: GRAFICO_4

```
SELECT SUM(VALOR_VENDIDO) AS total_vendido_2025
FROM workspace.gold_vinhos.fato_vendas_concluidas
WHERE ANO = 2025 AND TIPO_VINHO = 'Espumante';
```

5. Indicador
Consumo – França (unidades)

Dataset: GRAFICO_5

```
SELECT SUM(QUANTIDADE_VENDIDA) AS total_vendido_2025
FROM workspace.gold_vinhos.fato_vendas_concluidas
WHERE PAIS = 'França' AND ANO = 2025;
```

6. Gráfico de Pizza
Percentual de vendas por tipo de vinho – 2025

Dataset: GRAFICO_6

```
SELECT
    TIPO_VINHO,
    SUM(VALOR_VENDIDO) AS total_vendas,
    SUM(VALOR_VENDIDO) * 100.0 / SUM(SUM(VALOR_VENDIDO)) OVER() AS percentual_vendas
FROM workspace.gold_vinhos.fato_vendas_concluidas
WHERE ano = 2025
GROUP BY TIPO_VINHO
ORDER BY total_vendas DESC;
```

- Título da Página

O dashboard inclui um título estilizado:
``` Análise de Faturamento Anual de Vinhos```


- Página de Filtros Globais

O projeto também possui uma página dedicada a filtros globais:

- Filtro selecionável (drop-down)
- Aplicável aos gráficos da página principal
- Facilita a análise por ano, tipo de vinho, país etc.
