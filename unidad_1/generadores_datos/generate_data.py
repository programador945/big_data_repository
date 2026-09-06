import csv
import random
import sys
from datetime import datetime, timedelta


# ---------------------------------------------------------
# Configuración
# ---------------------------------------------------------

ARCHIVO = "ventas.csv"

clientes = [
    "Cliente_001",
    "Cliente_002",
    "Cliente_003",
    "Cliente_004",
    "Cliente_005",
    "Cliente_006",
    "Cliente_007",
    "Cliente_008",
    "Cliente_009",
    "Cliente_010"
]

productos = [
    "Laptop",
    "Monitor",
    "Teclado",
    "Mouse",
    "Impresora",
    "Tablet",
    "Celular",
    "Audifonos",
    "Webcam",
    "Disco_SSD"
]

ciudades = [
    "Bogota",
    "Medellin",
    "Cali",
    "Bucaramanga",
    "Barranquilla",
    "Cartagena",
    "Pereira",
    "Manizales"
]

categorias = [
    "Computadores",
    "Perifericos",
    "Telefonia",
    "Impresion",
    "Almacenamiento"
]

metodos_pago = [
    "Tarjeta_Credito",
    "Tarjeta_Debito",
    "Efectivo",
    "Transferencia"
]

canales = [
    "Tienda",
    "Web",
    "App",
    "Call_Center"
]

fecha_inicio = datetime(2025, 1, 1)


# ---------------------------------------------------------
# Función para generar datos
# ---------------------------------------------------------

def generar_ventas(cantidad):

    archivo = f"ventas_{cantidad}.csv"

    print(f"Generando {cantidad:,} registros...")
    print(f"Archivo: {archivo}")

    with open(
        archivo,
        "w",
        newline="",
        encoding="utf-8",
        buffering=1024 * 1024
    ) as f:

        writer = csv.writer(f)

        # Encabezado
        writer.writerow([
            "id_venta",
            "fecha",
            "cliente",
            "producto",
            "categoria",
            "cantidad",
            "precio_unitario",
            "descuento",
            "total",
            "ciudad",
            "metodo_pago",
            "canal",
            "vendedor",
            "estado"
        ])

        for id_venta in range(1, cantidad + 1):

            fecha = fecha_inicio + timedelta(
                days=random.randint(0, 364)
            )

            cantidad_producto = random.randint(1, 10)

            precio_unitario = round(
                random.uniform(10, 2000),
                2
            )

            descuento = round(
                random.uniform(0, 0.20),
                2
            )

            total = round(
                cantidad_producto
                * precio_unitario
                * (1 - descuento),
                2
            )

            writer.writerow([
                id_venta,
                fecha.strftime("%Y-%m-%d"),
                random.choice(clientes),
                random.choice(productos),
                random.choice(categorias),
                cantidad_producto,
                precio_unitario,
                descuento,
                total,
                random.choice(ciudades),
                random.choice(metodos_pago),
                random.choice(canales),
                f"Vendedor_{random.randint(1, 50):03d}",
                random.choice([
                    "Completada",
                    "Pendiente",
                    "Cancelada"
                ])
            ])

            # Mostrar progreso cada millón
            if id_venta % 1_000_000 == 0:
                print(
                    f"  {id_venta:,} registros generados..."
                )

    print("\nGeneración finalizada.")

    print(f"Archivo: {archivo}")

    print(
        f"Registros: {cantidad:,}"
    )


# ---------------------------------------------------------
# Programa principal
# ---------------------------------------------------------

if len(sys.argv) != 2:

    print(
        "Uso: python generar_ventas.py <cantidad_registros>"
    )

    print("\nEjemplos:")
    print("  python generar_ventas.py 10000")
    print("  python generar_ventas.py 100000")
    print("  python generar_ventas.py 1000000")
    print("  python generar_ventas.py 10000000")
    print("  python generar_ventas.py 100000000")

    sys.exit(1)


try:

    cantidad = int(sys.argv[1])

    if cantidad <= 0:
        raise ValueError

except ValueError:

    print(
        "Error: la cantidad debe ser un número entero positivo."
    )

    sys.exit(1)


generar_ventas(cantidad)