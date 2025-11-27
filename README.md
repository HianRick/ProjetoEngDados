# Projeto de Engenharia de Dados 

A documentação completa do projeto encontra-se disponível no [MKDOCS](https://HianRick.github.io/ProjetoEngDados/).

Este projeto demonstra um pipeline simples de engenharia de dados, incluindo:

- Geração de dados sintéticos com Faker
- Armazenamento de tabelas simuladas
- Efetuar leitura, padronização e tratamento de dados
- Manipulação e análise usando notebooks
- Abordagem educacional para prática de fundamentos
- Construir documentação estruturada utilizando MkDocs

# Objetivos do Projeto

Este projeto tem como finalidade desenvolver habilidades fundamentais em Engenharia de Dados, por meio da criação de conjuntos de dados fictícios, do tratamento e análise dessas informações e da documentação estruturada do processo.

A base do projeto envolve um conjunto de notebooks em Python e rotinas de geração de dados simulados utilizando a biblioteca Faker, com posterior limpeza, consolidação e exploração analítica dos dados referentes a um cenário fictício de vendas de vinhos.

# Tecnologias utilizadas

Tecnologia | Finalidade
-----------|------------
Python 3.x | Processamento e geração dos dados
Faker | Criação de dados simulados realistas
Pandas | Tratamento, limpeza e organização
Jupyter Notebook | Execução interativa das análises
MkDocs | Documentação do projeto

# Estrutura do Repositório

ProjetoEngDados/
│
├── docs/                          
├── ProjetoEngDados/               
├── tabelas/                       
│   ├── tabela_vendas_vinhos_1.csv
│   ├── tabela_vendas_vinhos_2.csv
│   ├── tabela_vendas_vinhos_3.csv
│   ├── tabela_vendas_vinhos_4.csv
│   ├── tabela_vendas_vinhos_5.csv
│   ├── tabela_vendas_vinhos_6.csv
│   ├── tabela_vendas_vinhos_7.csv
│   ├── tabela_vendas_vinhos_8.csv
│   ├── tabela_vendas_vinhos_9.csv
│   └── tabela_vendas_vinhos_10.csv
│
├── BibliotecaFaker.py             
├── Conexao.ipynb                  
├── Vinhos 001.ipynb               
├── Vinhos 002.ipynb               
├── Vinhos 003 - Silver.ipynb      
├── Vinhos 004 - Gold.ipynb        
│
├── mkdocs.yml                     
└── README.md                      



# Como executar o projeto

1. Clone o repositorio
   
   ```bash
   git clone https://github.com/HianRick/ProjetoEngDados.git
   cd ProjetoEngDados
   ```

2. Instale as dependências 

    ```bash
    pip install pandas faker matplotlib jupyter
    ```

3. Gere os dados fictícios 

    ```bash
    python BibliotecaFaker.py
    ```

4. Execute os notebooks

    ```bash
    jupyter notebook
    ```

Abra os arquivos: 

* Conexao.ipynb
* Vinhos 001.ipynb
* Vinhos 002.ipynb
* Vinhos 003 - Silver.ipynb
* Vinhos 004 - Gold.ipynb




