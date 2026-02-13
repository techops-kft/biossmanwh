import traceback
from psycopg2.extras import execute_values
from collections import OrderedDict

# 🔹 SQL de inserción (PostgreSQL)
SQL_INSERT = """
    INSERT INTO fact_articulos_venta_no_realizada_folios (
        id_folio,
        id_articulo,
        id_motivo,
        cantidad
    )
    VALUES %s
"""

def insertar_articulos_venta_no_realizada_folios(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT (SQL Server)
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

        # ---------- DESTINO
        # Obtener id_folio existentes
        cur_destino.execute("SELECT id_folio FROM fact_articulos_venta_no_realizada_folios")
        folios_existentes = set(row[0] for row in cur_destino.fetchall())
        print(f"Registros destino: {len(folios_existentes)}")

        # ---------- TRANSFORM
        # Filtrar solo nuevos y deduplicar
        folios_dict = OrderedDict()

        for r in folios_origen:
            IdFolio = r[0]

            if IdFolio not in folios_existentes and IdFolio not in folios_dict:
                folios_dict[IdFolio] = (
                    r[0],  # IdFolio
                    r[1],  # IdArticulo
                    r[2],  # IdMotivo
                    r[3]   # Cantidad
                )

        nuevos_folios = list(folios_dict.values())

        if not nuevos_folios:
            print("ℹ️ No hay nuevos folios para insertar.")
            return {
                "estatus": "success",
                "tabla": "fact_articulos_venta_no_realizada_folios",
                "proceso": "insertar_articulos_venta_no_realizada_folios",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # ---------- LOAD
        print(f"Insertando {len(nuevos_folios)} nuevos folios")
        execute_values(cur_destino, SQL_INSERT, nuevos_folios)
        conn_destino.commit()

        return {
            "estatus": "success",
            "tabla": "fact_articulos_venta_no_realizada_folios",
            "proceso": "insertar_articulos_venta_no_realizada_folios",
            "registros_insertados": len(nuevos_folios),
            "error_text": "No error"
        }

    except Exception:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "fact_articulos_venta_no_realizada_folios",
            "proceso": "insertar_articulos_venta_no_realizada_folios",
            "registros_insertados": 0,
            "error_text": traceback.format_exc()
        }