# Banco de Dados e Modelo ER


Esta página descreve as tabelas, chaves e relacionamentos principais.


> **Nota:** se você tiver um diagrama ER (arquivo .png/.drawio), coloque-o em `docs/assets/` e referencie aqui.


## Tabelas (exemplo)


- **clientes**
- id_cliente (PK)
- nome
- email
- data_cadastro


- **pedidos**
- id_pedido (PK)
- id_cliente (FK -> clientes.id_cliente)
- data_pedido
- valor_total


- **itens_pedido**
- id_item (PK)
- id_pedido (FK -> pedidos.id_pedido)
- produto
- quantidade
- preco_unitario


## Como gerar o diagrama ER


1. Exportar esquema do banco (DDL) ou criar um arquivo `.drawio`.
2. Salvar imagem em `docs/assets/er_diagrama.png`.
3. Inserir imagem usando Markdown: `![ER Diagram](assets/er_diagrama.