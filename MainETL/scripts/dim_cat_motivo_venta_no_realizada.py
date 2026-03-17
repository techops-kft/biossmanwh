import traceback
from psycopg2.extras import execute_values
from collections import OrderedDict

# 🔹 SQL INSERT (PostgreSQL)
SQL_INSERT = """
    INSERT INTO dim_cat_motivo_venta_no_realizada (
        id_motivo,
        descripcion,
        activo,
        created_at
    )
    VALUES %s
"""

def insertar_motivo_venta_no_realizada(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT (SQL Server)
        cur_origen.execute("""
            SELECT
                IdMotivo,
                Descripcion,
                Activo,
                FechaCreacion
            FROM dbo.CatMotivoVentaNoRealizada;
        """)

        motivos_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(motivos_origen)}")

        # ---------- DESTINO
        cur_destino.execute("SELECT id_motivo FROM dim_cat_motivo_venta_no_realizada")
        motivos_existentes = set(row[0] for row in cur_destino.fetchall())
        print(f"Registros destino: {len(motivos_existentes)}")

        # ---------- TRANSFORM
        motivos_dict = OrderedDict()

        for r in motivos_origen:
            IdMotivo = r[0]

            if IdMotivo not in motivos_existentes and IdMotivo not in motivos_dict:
                motivos_dict[IdMotivo] = (
                    r[0],  # IdMotivo
                    r[1],  # Descripcion
                    r[2],  # Activo
                    r[3],  # FechaCreacion
                )

        nuevos_motivos = list(motivos_dict.values())

        if not nuevos_motivos:
            print("ℹ️ No hay nuevos folios para insertar.")
            return {
                "estatus": "success",
                "tabla": "dim_cat_motivo_venta_no_realizada",
                "proceso": "insertar_motivo_venta_no_realizada",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # ---------- LOAD
        print(f"Insertando {len(nuevos_motivos)} nuevos motivos")
        execute_values(cur_destino, SQL_INSERT, nuevos_motivos)
        conn_destino.commit()

        return {
            "estatus": "success",
            "tabla": "dim_cat_motivo_venta_no_realizada",
            "proceso": "insertar_motivo_venta_no_realizada",
            "registros_insertados": len(nuevos_motivos),
            "error_text": "No error"
        }

    except Exception:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "dim_cat_motivo_venta_no_realizada",
            "proceso": "insertar_motivo_venta_no_realizada",
            "registros_insertados": 0,
            "error_text": traceback.format_exc()
        }



def actualizar_motivo_venta_no_realizada(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT DATA
        cur_origen.execute("""
            SELECT
                IdMotivo,
                Descripcion,
                Activo
            FROM dbo.CatMotivoVentaNoRealizada;
        """)

        motivos_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(motivos_origen)}")

        cur_destino.execute("""
            SELECT
                id_motivo,
                descripcion,
                activo
            FROM dim_cat_motivo_venta_no_realizada;
        """)

        motivos_destino = cur_destino.fetchall()
        print(f"Registros destino: {len(motivos_destino)}")

        # ---------- TRANSFORM
        mapa_destino = {
            m[0]: (m[1], m[2])
            for m in motivos_destino
        }

        actualizaciones = []

        for motivo in motivos_origen:
            id_motivo = motivo[0]
            descripcion_origen = motivo[1]
            activo_origen = motivo[2]

            datos_destino = mapa_destino.get(id_motivo)

            if datos_destino:
                descripcion_destino, activo_destino = datos_destino

                if (
                    descripcion_destino != descripcion_origen or
                    activo_destino != activo_origen
                ):
                    actualizaciones.append((
                        id_motivo,
                        descripcion_origen,
                        activo_origen
                    ))

                    # ---------- DEBUG
                    print(
                        f"Motivo {id_motivo}: "
                        f"descripcion {descripcion_destino}→{descripcion_origen}, "
                        f"activo {activo_destino}→{activo_origen}"
                    )

        # ---------- LOAD
        if actualizaciones:

            query = """
                WITH datos (
                    id_motivo,
                    descripcion,
                    activo
                ) AS (
                    VALUES %s
                )
                UPDATE dim_cat_motivo_venta_no_realizada dm
                SET
                    descripcion = datos.descripcion,
                    activo = datos.activo
                FROM datos
                WHERE dm.id_motivo = datos.id_motivo;
            """

            execute_values(cur_destino, query, actualizaciones)
            conn_destino.commit()

            registros_actualizados = len(actualizaciones)
            print(f"Se actualizaron {registros_actualizados} motivos.")

        else:
            print("No hubo motivos por actualizar.")
            registros_actualizados = 0

        return {
            "estatus": "success",
            "tabla": "dim_cat_motivo_venta_no_realizada",
            "proceso": "actualizar_motivo_venta_no_realizada",
            "registros_insertados": registros_actualizados,
            "error_text": "No error"
        }

    except Exception:
        conn_destino.rollback()

        return {
            "estatus": "failed",
            "tabla": "dim_cat_motivo_venta_no_realizada",
            "proceso": "actualizar_motivo_venta_no_realizada",
            "registros_insertados": 0,
            "error_text": traceback.format_exc()
        }