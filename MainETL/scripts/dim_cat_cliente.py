import traceback
from psycopg2.extras import execute_values
from collections import OrderedDict

# 🔹 SQL INSERT (PostgreSQL)
SQL_INSERT = """
    INSERT INTO dim_cat_cliente (
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
        origen,
        created_at
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
                Origen,
                CreationDate
            FROM dbo.CatClientes;
        """)

        clientes_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(clientes_origen)}")

        # ---------- DESTINO
        cur_destino.execute("SELECT id_cliente FROM dim_cat_cliente")
        clientes_existentes = set(row[0] for row in cur_destino.fetchall())
        print(f"Registros destino: {len(clientes_existentes)}")

        # ---------- TRANSFORM
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
                    r[11], # CreationDate
                )

        nuevos_clientes = list(clientes_dict.values())

        if not nuevos_clientes:
            print("ℹ️ No hay nuevos clientes para insertar.")
            return {
                "estatus": "success",
                "tabla": "dim_cat_cliente",
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
            "tabla": "dim_cat_cliente",
            "proceso": "insertar_cliente",
            "registros_insertados": len(nuevos_clientes),
            "error_text": "No error"
        }

    except Exception:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "dim_cat_cliente",
            "proceso": "insertar_cliente",
            "registros_insertados": 0,
            "error_text": traceback.format_exc()
        }


def actualizar_cliente(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT DATA
        cur_origen.execute("""
            SELECT
                IdCliente,
                Activo,
                NombreAlternativo,
                Origen
            FROM dbo.CatClientes;
        """)

        clientes_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(clientes_origen)}")

        cur_destino.execute("""
            SELECT
                id_cliente,
                activo,
                nombre_alternativo,
                origen
            FROM dim_cat_cliente;
        """)

        clientes_destino = cur_destino.fetchall()
        print(f"Registros destino: {len(clientes_destino)}")


        # ---------- TRANSFORM
        mapa_destino = {
            c[0]: (c[1], c[2], c[3])
            for c in clientes_destino
        }

        actualizaciones = []

        for cliente in clientes_origen:
            id_cliente = cliente[0]
            activo_origen = cliente[1]
            nombre_alt_origen = cliente[2]
            origen_origen = cliente[3]

            datos_destino = mapa_destino.get(id_cliente)

            if datos_destino:
                activo_destino, nombre_alt_destino, origen_destino = datos_destino

                if (
                    activo_destino != activo_origen or
                    nombre_alt_destino != nombre_alt_origen or
                    origen_destino != origen_origen
                ):
                    actualizaciones.append((
                        id_cliente,
                        activo_origen,
                        nombre_alt_origen,
                        origen_origen
                    ))

                    # ---------- DEBUG
                    print(
                        f"Cliente {id_cliente}: "
                        f"activo {activo_destino}→{activo_origen}, "
                        f"nombre_alt {nombre_alt_destino}→{nombre_alt_origen}, "
                        f"origen {origen_destino}→{origen_origen}"
                    )

        # ---------- LOAD
        if actualizaciones:

            query = """
                WITH datos (
                    id_cliente,
                    activo,
                    nombre_alternativo,
                    origen
                ) AS (
                    VALUES %s
                )
                UPDATE dim_cat_cliente dc
                SET
                    activo = datos.activo,
                    nombre_alternativo = datos.nombre_alternativo,
                    origen = datos.origen
                FROM datos
                WHERE dc.id_cliente = datos.id_cliente;
            """

            execute_values(cur_destino, query, actualizaciones)
            conn_destino.commit()

            registros_actualizados = len(actualizaciones)
            print(f"Se actualizaron {registros_actualizados} clientes.")

        else:
            print("No hubo clientes por actualizar.")
            registros_actualizados = 0

        return {
            "estatus": "success",
            "tabla": "dim_cat_cliente",
            "proceso": "actualizar_cliente",
            "registros_insertados": registros_actualizados,
            "error_text": "No error"
        }

    except Exception:
        conn_destino.rollback()

        return {
            "estatus": "failed",
            "tabla": "dim_cat_cliente",
            "proceso": "actualizar_cliente",
            "registros_insertados": 0,
            "error_text": traceback.format_exc()
        }