"""
Module: fake_data_generator
Purpose: Create fake csv data for the excel_to_sql_pipeline

This module is part of the Fynbyte toolkit.
"""

__author__ = "Juliana Albertyn"
__email__ = "julie_albertyn@yahoo.com"
__status__ = "development"  # or testing or production
__date__ = "2026-02-16"

import random
import pandas as pd
from faker import Faker
from typing import Any

fake = Faker()

def fake_city() -> str:
    sa_cities = [
        "Cape Town",
        "Johannesburg",
        "Durban",
        "Pretoria",
        "Port Elizabeth",
        "Bloemfontein",
        "East London",
        "Polokwane",
        "Nelspruit",
        "Kimberley"
    ]
    return random.choice(sa_cities)


def fake_mobile_number() -> str:
    prefixes = ["061", "063", "071", "082", "083", "080"]
    prefix = random.choice(prefixes)

    # choose total length (10, 9, 11, etc.)
    total_length = random.choice([9, 10])

    # generate remaining digits
    remaining = total_length - len(prefix)
    digits = "".join(random.choices("0123456789", k=remaining))

    return f"{prefix}{digits}"


def fake_customer_name() -> str:
    # Names: leading and trailing whitespace or junk chars
    words = []
    s = fake.name()
    if random.randint(1, 100) % 7 == 0:
        s = " " + s + "  "
    elif random.randint(1, 100) % 25 == 0:
        words = s.split(" ")
        s = words[0] + random.choice(["!", "@", "?", "#", "%"]) + words[1]
    return s


def fake_date() -> Any:
    # Dates: different formats, sometimes invalid/missing
    date_options = [
        fake.date(),  # YYYY-MM-DD
        fake.date_this_century().strftime("%d/%m/%Y"),  # DD/MM/YYYY
        fake.date_this_decade().strftime("%m-%d-%Y"),  # MM-DD-YYYY
        fake.date_this_year().strftime("%d-%b-%Y"),  # 07-Feb-2026
        "",  # missing
    ]
    return random.choice(date_options)

def fake_product() -> str:
    adjectives = ["Premium", "Advanced", "Eco", "Smart", "Portable"]
    nouns = ["Keyboard", "Laptop", "Chair", "Monitor", "Headphones"]    

    return random.choice(adjectives) + ' ' + random.choice(nouns)

def fake_amount() -> str:
    # Amounts: sometimes integers, sometimes floats, sometimes with currency symbols
    amount_options = [
        str(random.randint(10, 5000)),
        f"{random.uniform(10, 5000):.2f}",
        f"R{random.randint(10, 5000)}",  # South African Rand style
        "",  # simulate missing value
    ]
    return random.choice(amount_options)


# Create fake data for two sheets
data_customers = {
    "id": [i for i in range(1, 21)],
    "name": [fake_customer_name() for _ in range(20)],
    "email": [fake.email() for _ in range(20)],
    "mobile": [fake_mobile_number() for _ in range(20)],
    "city": [fake_city() for _ in range(20)],
    "join_date": [fake_date() for _ in range(20)],
}

data_products = {
    "product_id": [i for i in range(1001, 1011)],
    "product_name": [fake_product() for _ in range(10)],
    "product_price": [fake_amount() for _ in range(10)],
    "stock_quantity": [random.randint(1, 100) for _ in range(10)],
}

data_orders = {
    "order_id": [i for i in range(101, 121)],
    "customer_id": [i for i in range(1, 21)],
    "order_date": [fake_date() for _ in range(20)],
}

data_order_items = []
for i in range(2000, 2100):
    quantity = fake.random_int(min=1, max=5)
    unit_price = fake_amount()
    try:
        total_price = quantity * float(unit_price)
        if fake.boolean(chance_of_getting_true=10):
            total_price += fake.random_int(min=1, max=50)
    except Exception:
        total_price = 0

    row = {
        "order_item_id": i,
        "order_id": fake.random_int(min=101, max=120),
        "product_id": fake.random_int(min=1000, max=1010),
        "quantity": quantity,
        "unit_price": unit_price,
        "total_price": total_price,
    }
    data_order_items.append(row)

df_customers = pd.DataFrame(data_customers)
df_product = pd.DataFrame(data_products)
df_orders = pd.DataFrame(data_orders)
df_order_items = pd.DataFrame(data_order_items)

# Write to Excel with four sheets
with pd.ExcelWriter("fake_data.xlsx") as writer:
    df_customers.to_excel(writer, sheet_name="Customers", index=False)
    df_product.to_excel(writer, sheet_name="Products", index=False)
    df_orders.to_excel(writer, sheet_name="Orders", index=False)
    df_order_items.to_excel(writer, sheet_name="OrderItems", index=False)


if __name__ == "__main__":
    print(fake_product())
