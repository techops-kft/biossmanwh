import traceback
from psycopg2.extras import execute_values
from datetime import time

# ---------------------------------------------------------
# Determinar turno
# ---------------------------------------------------------
def obtener_turno(fecha):
    if fecha is None:
        return None

    hora = fecha.time()

    if time(7,0) <= hora < time(14,0):
        return "Diurno"

    elif time(14,0) <= hora < time(21,0):
        return "Vespertino"

    else:
        return "Nocturno"

SQL_INSERT = """
    INSERT INTO fact_articulos_venta_no_realizada_folios (
        id_folio,
        id_material,
        id_motivo,
        cantidad,
        created_at,
        turno
    )
    VALUES %s
"""

def insertar_articulos_venta_no_realizada_folios(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT ORIGEN
        cur_origen.execute("""
            SELECT
                IdFolio,
                IdArticulo,
                IdMotivo,
                Cantidad,
                CreationDate
            FROM dbo.ArticulosVentaNoRealizadaFolios;
        """)

        folios_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(folios_origen)}")

        # ---------- EXTRACT DESTINO
        cur_destino.execute("""
            SELECT
                id_folio,
                id_material
            FROM fact_articulos_venta_no_realizada_folios;
        """)

        folios_destino = set((f[0], f[1]) for f in cur_destino.fetchall())
        print(f"Registros destino: {len(folios_destino)}")

        # ---------- TRANSFORM
        nuevos_registros = []

        for r in folios_origen:

            clave = (r[0], r[1])

            if clave not in folios_destino:

                turno = obtener_turno(r[4])

                nuevos_registros.append((
                    r[0],
                    r[1],
                    r[2],
                    r[3],
                    r[4],
                    turno
                ))

        if not nuevos_registros:
            print("ℹ️ No hay registros nuevos para insertar.")

            return {
                "estatus": "success",
                "tabla": "fact_articulos_venta_no_realizada_folios",
                "proceso": "insertar_articulos_venta_no_realizada_folios",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # ---------- LOAD
        print(f"Insertando {len(nuevos_registros)} registros nuevos")

        execute_values(cur_destino, SQL_INSERT, nuevos_registros)

        conn_destino.commit()

        return {
            "estatus": "success",
            "tabla": "fact_articulos_venta_no_realizada_folios",
            "proceso": "insertar_articulos_venta_no_realizada_folios",
            "registros_insertados": len(nuevos_registros),
            "error_text": "No error"
        }

    except Exception:

        conn_destino.rollback()
        error_trace = traceback.format_exc()

        print("❌ Error durante el proceso")
        print(error_trace)

        return {
            "estatus": "failed",
            "tabla": "fact_articulos_venta_no_realizada_folios",
            "proceso": "insertar_articulos_venta_no_realizada_folios",
            "registros_insertados": 0,
            "error_text": error_trace
        }



def actualizar_articulos_venta_no_realizada_folios(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT DATA (ORIGEN)
        cur_origen.execute("""
            SELECT
                IdFolio,
                IdArticulo,
                IdMotivo,
                Cantidad
            FROM dbo.ArticulosVentaNoRealizadaFolios;
        """)

        folios_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(folios_origen)}")

        # ---------- EXTRACT DATA (DESTINO)
        cur_destino.execute("""
            SELECT
                id_folio,
                id_material,
                id_motivo,
                cantidad
            FROM fact_articulos_venta_no_realizada_folios;
        """)

        folios_destino = cur_destino.fetchall()
        print(f"Registros destino: {len(folios_destino)}")

        # ---------- TRANSFORM
        # Mapa con clave compuesta
        mapa_destino = {
            (f[0], f[1]): (f[2], f[3])
            for f in folios_destino
        }

        actualizaciones = []

        for folio in folios_origen:

            id_folio = folio[0]
            id_material = folio[1]
            motivo_origen = folio[2]
            cantidad_origen = folio[3]

            datos_destino = mapa_destino.get((id_folio, id_material))

            if datos_destino:
                motivo_destino, cantidad_destino = datos_destino

                if (
                    motivo_destino != motivo_origen or
                    cantidad_destino != cantidad_origen
                ):
                    actualizaciones.append((
                        id_folio,
                        id_material,
                        motivo_origen,
                        cantidad_origen
                    ))

                    # ---------- DEBUG
                    print(
                        f"Folio {id_folio} | Material {id_material}: "
                        f"motivo {motivo_destino}→{motivo_origen}, "
                        f"cantidad {cantidad_destino}→{cantidad_origen}"
                    )

        # ---------- LOAD
        if actualizaciones:

            query = """
                WITH datos (
                    id_folio,
                    id_material,
                    id_motivo,
                    cantidad
                ) AS (
                    VALUES %s
                )
                UPDATE fact_articulos_venta_no_realizada_folios fa
                SET
                    id_motivo = datos.id_motivo,
                    cantidad = datos.cantidad
                FROM datos
                WHERE
                    fa.id_folio = datos.id_folio
                    AND fa.id_material = datos.id_material;
            """

            execute_values(cur_destino, query, actualizaciones)
            conn_destino.commit()

            registros_actualizados = len(actualizaciones)
            print(f"Se actualizaron {registros_actualizados} registros.")

        else:
            print("No hubo registros por actualizar.")
            registros_actualizados = 0

        return {
            "estatus": "success",
            "tabla": "fact_articulos_venta_no_realizada_folios",
            "proceso": "actualizar_articulos_venta_no_realizada_folios",
            "registros_insertados": registros_actualizados,
            "error_text": "No error"
        }

    except Exception:
        conn_destino.rollback()
        error_trace = traceback.format_exc()

        print("❌ Error durante el proceso")
        print(error_trace)

        return {
            "estatus": "failed",
            "tabla": "fact_articulos_venta_no_realizada_folios",
            "proceso": "actualizar_articulos_venta_no_realizada_folios",
            "registros_insertados": 0,
            "error_text": error_trace
        }