import traceback
from psycopg2.extras import execute_values
from collections import OrderedDict

# 🔹 SQL de inserción (PostgreSQL)
SQL_INSERT = """
    INSERT INTO dim_motivo_venta_no_realizada (
        id_motivo,
        descripcion,
        activo
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
                Activo
            FROM dbo.CatMotivoVentaNoRealizada;
        """)

        motivos_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(motivos_origen)}")

        # ---------- DESTINO
        # Obtener id_motivo existentes
        cur_destino.execute("SELECT id_motivo FROM dim_motivo_venta_no_realizada")
        motivos_existentes = set(row[0] for row in cur_destino.fetchall())
        print(f"Registros destino: {len(motivos_existentes)}")

        # ---------- TRANSFORM
        # Filtrar solo nuevos y deduplicar
        motivos_dict = OrderedDict()

        for r in motivos_origen:
            IdMotivo = r[0]

            if IdMotivo not in motivos_existentes and IdMotivo not in motivos_dict:
                motivos_dict[IdMotivo] = (
                    r[0],  # IdMotivo
                    r[1],  # Descripcion
                    r[2]  # Activo
                )

        nuevos_motivos = list(motivos_dict.values())

        if not nuevos_motivos:
            print("ℹ️ No hay nuevos folios para insertar.")
            return {
                "estatus": "success",
                "tabla": "dim_motivo_venta_no_realizada",
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
            "tabla": "dim_motivo_venta_no_realizada",
            "proceso": "insertar_motivo_venta_no_realizada",
            "registros_insertados": len(nuevos_motivos),
            "error_text": "No error"
        }

    except Exception:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "dim_motivo_venta_no_realizada",
            "proceso": "insertar_motivo_venta_no_realizada",
            "registros_insertados": 0,
            "error_text": traceback.format_exc()
        }