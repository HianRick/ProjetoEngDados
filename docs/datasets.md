# Dataset Fictício — Vendas de Vinhos

O dataset deste projeto foi totalmente gerado de forma sintética utilizando a biblioteca **Faker**, simulando um cenário realista de vendas de vinhos, com informações completas de cliente, produto e venda.
Cada linha representa uma transação individual.

---

## Estrutura dos Dados

Campos presentes no dataset:

- `id_venda`
- `data_venda`
- `cliente_id`
- `cliente_nome`
- `cliente_email`
- `cliente_cidade`
- `cliente_estado`
- `tipo_vinho`
- `rotulo`
- `uva`
- `pais_origem`
- `teor_alcoolico`
- `safra`
- `preco`
- `quantidade`
- `estabelecimento`
- `status_venda`

---

## Geração dos dados

• O script responsável é o arquivo BibliotecaFaker.py.
• Ele gera automaticamente 10 arquivos CSV.
• Cada arquivo contém 20.000 registros.
• O total do dataset completo é de 200.000 linhas.

## Localização dos arquivos:

• Todos os CSV são salvos na pasta tabelas.
• Os nomes seguem o padrão tabela_vendas_vinhos_1.csv até tabela_vendas_vinhos_10.csv.

## Utilização no projeto:

• Esses arquivos são lidos inicialmente no notebook Conexao.ipynb.
• Em seguida passam por tratamento e padronização no Vinhos 001.
• São analisados no Vinhos 002.
• Depois seguem para as camadas Silver e Gold nos notebooks seguintes.

