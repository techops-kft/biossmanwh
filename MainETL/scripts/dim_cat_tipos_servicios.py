import traceback
from psycopg2.extras import execute_values
from collections import OrderedDict

# 🔹 SQL INSERT (PostgreSQL)
SQL_INSERT = """
    INSERT INTO dim_cat_tipos_servicios (
        id_tipo_servicio,
        id_tipo_servicio_sbo,
        id_unidad_negocio,
        codigo,
        nombre,
        activo,
        created_at
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
                Activo,
                CreationDate
            FROM dbo.CatTiposServicio;
        """)

        tipos_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(tipos_origen)}")

        # ---------- DESTINO
        cur_destino.execute("SELECT id_tipo_servicio FROM dim_cat_tipos_servicios")
        tipos_existentes = set(row[0] for row in cur_destino.fetchall())
        print(f"Registros destino: {len(tipos_existentes)}")

        # ---------- TRANSFORM
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
                    r[6],  # CreationDate
                )

        nuevos_tipos = list(tipos_dict.values())

        if not nuevos_tipos:
            print("ℹ️ No hay nuevos tipos de servicio para insertar.")
            return {
                "estatus": "success",
                "tabla": "dim_cat_tipos_servicios",
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
            "tabla": "dim_cat_tipos_servicios",
            "proceso": "insertar_tipos_servicios",
            "registros_insertados": len(nuevos_tipos),
            "error_text": "No error"
        }

    except Exception:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "dim_cat_tipos_servicios",
            "proceso": "insertar_tipos_servicios",
            "registros_insertados": 0,
            "error_text": traceback.format_exc()
        }



def actualizar_tipos_servicios(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT DATA
        cur_origen.execute("""
            SELECT
                IdTipoServicio,
                Nombre,
                Activo
            FROM dbo.CatTiposServicio;
        """)

        tipos_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(tipos_origen)}")

        cur_destino.execute("""
            SELECT
                id_tipo_servicio,
                nombre,
                activo
            FROM dim_cat_tipos_servicios;
        """)

        tipos_destino = cur_destino.fetchall()
        print(f"Registros destino: {len(tipos_destino)}")

        # ---------- TRANSFORM
        mapa_destino = {
            t[0]: (t[1], t[2])
            for t in tipos_destino
        }

        actualizaciones = []

        for tipo in tipos_origen:
            id_tipo_servicio = tipo[0]
            nombre_origen = tipo[1]
            activo_origen = tipo[2]

            datos_destino = mapa_destino.get(id_tipo_servicio)

            if datos_destino:
                nombre_destino, activo_destino = datos_destino

                if (
                    nombre_destino != nombre_origen or
                    activo_destino != activo_origen
                ):
                    actualizaciones.append((
                        id_tipo_servicio,
                        nombre_origen,
                        activo_origen
                    ))

                    # ---------- DEBUG
                    print(
                        f"TipoServicio {id_tipo_servicio}: "
                        f"nombre {nombre_destino}→{nombre_origen}, "
                        f"activo {activo_destino}→{activo_origen}"
                    )

        # ---------- LOAD
        if actualizaciones:

            query = """
                WITH datos (
                    id_tipo_servicio,
                    nombre,
                    activo
                ) AS (
                    VALUES %s
                )
                UPDATE dim_cat_tipos_servicios dts
                SET
                    nombre = datos.nombre,
                    activo = datos.activo
                FROM datos
                WHERE dts.id_tipo_servicio = datos.id_tipo_servicio;
            """

            execute_values(cur_destino, query, actualizaciones)
            conn_destino.commit()

            registros_actualizados = len(actualizaciones)
            print(f"Se actualizaron {registros_actualizados} tipos de servicio.")

        else:
            print("No hubo tipos de servicio por actualizar.")
            registros_actualizados = 0

        return {
            "estatus": "success",
            "tabla": "dim_cat_tipos_servicios",
            "proceso": "actualizar_tipos_servicios",
            "registros_insertados": registros_actualizados,
            "error_text": "No error"
        }

    except Exception:
        conn_destino.rollback()

        return {
            "estatus": "failed",
            "tabla": "dim_cat_tipos_servicios",
            "proceso": "actualizar_tipos_servicios",
            "registros_insertados": 0,
            "error_text": traceback.format_exc()
        }