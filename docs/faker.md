# Geração de Dados com Faker

O arquivo **`BibliotecaFaker.py`** é responsável por criar dados sintéticos para simular vendas de vinhos.

### Principais funcionalidades

- Uso da biblioteca Faker
- Criação de múltiplas tabelas de vendas
- Campos gerados:

  - Nome do cliente  
  - Cidade  
  - Quantidade  
  - Tipo de vinho  
  - Valor da venda  
  - Data da compra  

### Trecho de código

```python
from faker import Faker
fake = Faker()

def gerar_dado():
    return {
        "cliente": fake.name(),
        "cidade": fake.city(),
        "vinho": fake.color_name(),
        "quantidade": fake.random_int(1, 12),
        "valor": fake.pyfloat(positive=True),
        "data_venda": fake.date_this_year()
    }
```

---

## Saída

Os dados gerados são salvos em arquivos CSV dentro da pasta `tabelas/`.
