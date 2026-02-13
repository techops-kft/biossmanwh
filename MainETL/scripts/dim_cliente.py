import traceback
from psycopg2.extras import execute_values
from collections import OrderedDict

# 🔹 SQL de inserción (PostgreSQL)
SQL_INSERT = """
    INSERT INTO dim_cliente (
        id_cliente,
        id_empresa,
        id_cliente_sbo,
        direccion,
        nombre,
        region,
        responsable,
        rfc,
        activo,
        nombre_alternativo,
        origen
    )
    VALUES %s
"""

def insertar_cliente(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT (SQL Server)
        cur_origen.execute("""
            SELECT
                IdCliente,
                IdEmpresa,
                IdClienteSBO,
                Direccion,
                Nombre,
                Region,
                Responsable,
                RFC,
                Activo,
                NombreAlternativo,
                Origen
            FROM dbo.CatClientes;
        """)

        clientes_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(clientes_origen)}")

        # ---------- DESTINO
        # Obtener id_cliente ya existentes
        cur_destino.execute("SELECT id_cliente FROM dim_cliente")
        clientes_existentes = set(row[0] for row in cur_destino.fetchall())
        print(f"Registros destino: {len(clientes_existentes)}")

        # ---------- TRANSFORM
        # Filtrar solo nuevos y deduplicar
        clientes_dict = OrderedDict()

        for r in clientes_origen:
            id_cliente = r[0]

            if id_cliente not in clientes_existentes and id_cliente not in clientes_dict:
                clientes_dict[id_cliente] = (
                    r[0],  # id_cliente
                    r[1],  # id_empresa
                    r[2],  # id_cliente_sbo
                    r[3],  # direccion
                    r[4],  # nombre
                    r[5],  # region
                    r[6],  # responsable
                    r[7],  # rfc
                    r[8],  # activo
                    r[9],  # nombre_alternativo
                    r[10], # origen
                )

        nuevos_clientes = list(clientes_dict.values())

        if not nuevos_clientes:
            print("ℹ️ No hay nuevos clientes para insertar.")
            return {
                "estatus": "success",
                "tabla": "dim_cliente",
                "proceso": "insertar_cliente",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # ---------- LOAD
        print(f"Insertando {len(nuevos_clientes)} nuevos clientes")
        execute_values(cur_destino, SQL_INSERT, nuevos_clientes)
        conn_destino.commit()

        return {
            "estatus": "success",
            "tabla": "dim_cliente",
            "proceso": "insertar_cliente",
            "registros_insertados": len(nuevos_clientes),
            "error_text": "No error"
        }

    except Exception:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "dim_cliente",
            "proceso": "insertar_cliente",
            "registros_insertados": 0,
            "error_text": traceback.format_exc()
        }