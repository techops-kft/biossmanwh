import traceback
from psycopg2.extras import execute_values
from collections import OrderedDict

# 🔹 SQL INSERT (PostgreSQL)
SQL_INSERT = """
    INSERT INTO dim_cat_almacenes (
        id_almacen,
        id_cliente,
        id_almacen_sbo,
        central,
        nombre,
        activo,
        created_at
    )
    VALUES %s
"""

def insertar_almacenes(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT
        cur_origen.execute("""
            SELECT
                IdAlmacen,
                IdCliente,
                IdAlmacenSBO,
                Central,
                Nombre,
                Activo,
                CreationDate
            FROM dbo.CatAlmacenes
        """)

        registros_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(registros_origen)}")

        # ---------- DESTINO
        cur_destino.execute("""
            SELECT id_almacen
            FROM dim_cat_almacenes
        """)

        registros_destino = set(r[0] for r in cur_destino.fetchall())
        print(f"Registros destino: {len(registros_destino)}")

        # ---------- TRANSFORM
        almacenes_dict = OrderedDict()

        for r in registros_origen:

            id_almacen = r[0]

            if id_almacen not in registros_destino and id_almacen not in almacenes_dict:

                almacenes_dict[id_almacen] = (

                    r[0],  # id_almacen
                    r[1],  # id_cliente
                    r[2],  # id_almacen_sbo
                    r[3],  # central
                    r[4],  # nombre
                    r[5],  # activo
                    r[6],  # created_at
                )

        nuevos_registros = list(almacenes_dict.values())

        if not nuevos_registros:

            print("ℹ️ No hay nuevos almacenes para insertar.")

            return {
                "estatus": "success",
                "tabla": "dim_cat_almacenes",
                "proceso": "insertar_almacenes",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # ---------- LOAD
        print(f"Insertando {len(nuevos_registros)} nuevos registros")
        execute_values(cur_destino, SQL_INSERT, nuevos_registros)
        conn_destino.commit()

        return {
            "estatus": "success",
            "tabla": "dim_cat_almacenes",
            "proceso": "insertar_almacenes",
            "registros_insertados": len(nuevos_registros),
            "error_text": "No error"
        }

    except Exception:

        conn_destino.rollback()

        return {
            "estatus": "failed",
            "tabla": "dim_cat_almacenes",
            "proceso": "insertar_almacenes",
            "registros_insertados": 0,
            "error_text": traceback.format_exc()
        }