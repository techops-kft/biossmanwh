import traceback
from psycopg2.extras import execute_values
from collections import OrderedDict

# 🔹 SQL de inserción (PostgreSQL)
SQL_INSERT = """
    INSERT INTO dim_tipos_servicios (
        id_tipo_servicio,
        id_tipo_servicio_sbo,
        id_unidad_negocio,
        codigo,
        nombre,
        activo
    )
    VALUES %s
"""

def insertar_tipos_servicios(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT (SQL Server)
        cur_origen.execute("""
            SELECT
                IdTipoServicio,
                IdTipoServicioSBO,
                IdUnidadNegocio,
                Codigo,
                Nombre,
                Activo
            FROM dbo.CatTiposServicio;
        """)

        tipos_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(tipos_origen)}")

        # ---------- DESTINO
        # Obtener id_tipo_servicio existentes
        cur_destino.execute("SELECT id_tipo_servicio FROM dim_tipos_servicios")
        tipos_existentes = set(row[0] for row in cur_destino.fetchall())
        print(f"Registros destino: {len(tipos_existentes)}")

        # ---------- TRANSFORM
        # Filtrar solo nuevos y deduplicar
        tipos_dict = OrderedDict()

        for r in tipos_origen:
            idtiposervicio = r[0]

            if idtiposervicio not in tipos_existentes and idtiposervicio not in tipos_dict:
                tipos_dict[idtiposervicio] = (
                    r[0],  # idtiposervicio
                    r[1],  # idtiposerviciosbo
                    r[2],  # idunidadnegocio
                    r[3],  # codigo
                    r[4],  # nombre
                    r[5],  # activo
                )

        nuevos_tipos = list(tipos_dict.values())

        if not nuevos_tipos:
            print("ℹ️ No hay nuevos tipos de servicio para insertar.")
            return {
                "estatus": "success",
                "tabla": "dim_tipos_servicios",
                "proceso": "insertar_tipos_servicios",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # ---------- LOAD
        print(f"Insertando {len(nuevos_tipos)} nuevos tipos de servicio")
        execute_values(cur_destino, SQL_INSERT, nuevos_tipos)
        conn_destino.commit()

        return {
            "estatus": "success",
            "tabla": "dim_tipos_servicios",
            "proceso": "insertar_tipos_servicios",
            "registros_insertados": len(nuevos_tipos),
            "error_text": "No error"
        }

    except Exception:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "dim_tipos_servicios",
            "proceso": "insertar_tipos_servicios",
            "registros_insertados": 0,
            "error_text": traceback.format_exc()
        }