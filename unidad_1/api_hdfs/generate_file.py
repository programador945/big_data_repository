import csv
import os
import random
from datetime import datetime, timedelta

ARCHIVO = "ventas_10mb.csv"
TAMANO_OBJETIVO = 10 * 1024 * 1024  # 10 MB

clientes = [
    "Cliente_001",
    "Cliente_002",
    "Cliente_003",
    "Cliente_004",
    "Cliente_005"
]

productos = [
    "Laptop",
    "Monitor",
    "Teclado",
    "Mouse",
    "Impresora"
]

fecha = datetime(2025, 1, 1)

with open(ARCHIVO, "w", newline="", encoding="utf-8") as archivo:

    writer = csv.writer(archivo)

    # Encabezado
    writer.writerow([
        "id_venta",
        "fecha",
        "cliente",
        "producto",
        "cantidad",
        "precio"
    ])

    id_venta = 1

    while os.path.getsize(ARCHIVO) < TAMANO_OBJETIVO:

        writer.writerow([
            id_venta,
            (fecha + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
            random.choice(clientes),
            random.choice(productos),
            random.randint(1, 10),
            round(random.uniform(10, 2000), 2)
        ])

        id_venta += 1

print(f"Archivo generado: {ARCHIVO}")
print(f"Tamaño: {os.path.getsize(ARCHIVO) / (1024 * 1024):.2f} MB")
print(f"Registros: {id_venta - 1}")