import traceback
from psycopg2.extras import execute_values
from collections import OrderedDict

# 🔹 SQL INSERT (PostgreSQL)
SQL_INSERT = """
    INSERT INTO dim_cat_unidades_negocio (
        id_unidad_negocio,
        id_empresa,
        id_unidad_negocio_sbo,
        nombre,
        activo,
        activo_programacion,
        abrev,
        created_at
    )
    VALUES %s
"""

def insertar_unidades_negocio(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT
        cur_origen.execute("""
            SELECT
                IdUnidadNegocio,
                IdEmpresa,
                IdUnidadNegocioSBO,
                Nombre,
                Activo,
                ActivoProgramacion,
                Abrev,
                CreationDate
            FROM dbo.CatUnidadesNegocio
        """)

        registros_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(registros_origen)}")

        # ---------- DESTINO
        cur_destino.execute("""
            SELECT id_unidad_negocio
            FROM dim_cat_unidades_negocio
        """)

        registros_destino = set(r[0] for r in cur_destino.fetchall())
        print(f"Registros destino: {len(registros_destino)}")

        # ---------- TRANSFORM
        unidades_dict = OrderedDict()

        for r in registros_origen:

            id_unidad = r[0]

            if id_unidad not in registros_destino and id_unidad not in unidades_dict:

                unidades_dict[id_unidad] = (

                    r[0],  # id_unidad_negocio
                    r[1],  # id_empresa
                    r[2],  # id_unidad_negocio_sbo
                    r[3],  # nombre
                    r[4],  # activo
                    r[5],  # activo_programacion
                    r[6],  # abrev
                    r[7],  # created_at
                )

        nuevos_registros = list(unidades_dict.values())

        if not nuevos_registros:

            print("ℹ️ No hay nuevas unidades de negocio para insertar.")

            return {
                "estatus": "success",
                "tabla": "dim_cat_unidades_negocio",
                "proceso": "insertar_unidades_negocio",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # ---------- LOAD
        print(f"Insertando {len(nuevos_registros)} nuevos registros")
        execute_values(cur_destino, SQL_INSERT, nuevos_registros)
        conn_destino.commit()

        return {
            "estatus": "success",
            "tabla": "dim_cat_unidades_negocio",
            "proceso": "insertar_unidades_negocio",
            "registros_insertados": len(nuevos_registros),
            "error_text": "No error"
        }

    except Exception:

        conn_destino.rollback()

        return {
            "estatus": "failed",
            "tabla": "dim_cat_unidades_negocio",
            "proceso": "insertar_unidades_negocio",
            "registros_insertados": 0,
            "error_text": traceback.format_exc()
        }