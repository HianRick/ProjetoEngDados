from faker import Faker #cria as informações falsas
import pandas as pd #manipulação de dados em csv
import numpy as np #operações matemáticas
from datetime import datetime, timedelta #manipulação de datas
import random #geração de números aleatórios

fake = Faker(locale='pt-BR') #configuração para português do Brasil
print()


def random_date_last_3_years():
    end = datetime.now()
    start = end - timedelta(days=3*365)
    return fake.date_between(start_date=start, end_date=end)

# Definição dos tipos de vinhos e suas características
wine_types = {
    "Tinto": [
        ("Cabernet Sauvignon", "Cabernet Sauvignon", "Chile", 13.5),
        ("Merlot Reserva", "Merlot", "França", 13.0),
        ("Malbec Premium", "Malbec", "Argentina", 14.2),
        ("Syrah Signature", "Syrah", "Austrália", 13.8)
    ],
    "Branco": [
        ("Chardonnay Gold", "Chardonnay", "EUA", 12.5),
        ("Sauvignon Blanc Fresh", "Sauvignon Blanc", "Nova Zelândia", 12.0),
        ("Riesling Classic", "Riesling", "Alemanha", 11.5)
    ],
    "Rosé": [
        ("Rosé Provence", "Blend", "França", 12.5),
        ("Rosé Summer", "Grenache", "Espanha", 12.0)
    ],
    "Espumante": [
        ("Prosecco Extra Dry", "Glera", "Itália", 11.0),
        ("Cava Brut", "Macabeo", "Espanha", 11.5),
        ("Champagne Brut", "Blend", "França", 12.0)
    ]
}


#Define uma função que gera uma tabela de venda
#n_rows: quantidade de linhas
#table_name: nome do arquivo CSV de saída
def generate_wine_sales_table(n_rows=20000, table_name="tabela_vinhos"):

    rows = []
    #cria uma lista vazia. Cada venda gerada será adicionada dentro desta lista.
    
    for _ in range(n_rows):
        # Escolhe tipo de vinho
        wine_type = random.choice(list(wine_types.keys()))
        wine_name, grape, country, alcohol = random.choice(wine_types[wine_type])
        
        # Preço realista por tipo
        base_price = {
            "Tinto": random.uniform(40, 150),
            "Branco": random.uniform(35, 120),
            "Rosé": random.uniform(30, 110),
            "Espumante": random.uniform(50, 300)
        }[wine_type]

        row = {
            "id_venda": fake.uuid4(),
            "data_venda": random_date_last_3_years(),
            "cliente_id": fake.uuid4(),
            "cliente_nome": fake.name(),
            "cliente_email": fake.email(),
            "cliente_cidade": fake.city(),
            "cliente_estado": fake.state_abbr(),
            
            # Dados do vinho
            "tipo_vinho": wine_type,
            "rotulo": wine_name,
            "uva": grape,
            "pais_origem": country,
            "teor_alcoolico": alcohol,
            "preco": round(base_price, 2),
            "quantidade": random.randint(1, 12),
            
            "status_venda": random.choice(["concluída", "pendente", "cancelada"])
        }
        
        rows.append(row)
    
    df = pd.DataFrame(rows)
    df.to_csv(f"{table_name}.csv", index=False)
    return df

tables = {}
for i in range(1, 11):
    name = f"tabela_vendas_vinhos_{i}"
    print(f"Gerando {name}...")
    tables[name] = generate_wine_sales_table(20000, name)