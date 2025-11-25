# Arquitetura Geral

A arquitetura do projeto é composta por:

```
[Python + Faker] → [Geração de datasets]
                     ↓
                  CSVs locais
                     ↓
                 Jupyter Notebook
                    Análises
```

### Componentes

| Componente | Função |
|---|---|
| `BibliotecaFaker.py` | Gera dados sintéticos com Faker |
| `tabelas/` | Guarda os CSVs gerados |
| Notebooks | Manipulam e analisam os dados |

---

## Objetivo

Criar uma experiência simples de ponta a ponta, reforçando:

- Criação de dados para testes
- Leitura e análise dos arquivos
- Aplicação de conceitos básicos de engenharia de dados
