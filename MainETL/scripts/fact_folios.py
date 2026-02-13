import traceback
from psycopg2.extras import execute_values
from collections import OrderedDict

# 🔹 SQL de inserción (PostgreSQL)
SQL_INSERT = """
    INSERT INTO fact_folios (
        id_folio,
        id_cliente,
        id_entrega_sbo,
        id_factura_sbo,
        id_servicio,
        id_terminal,
        tipo_cirugia_up,
        estatus_folio,
        estatus_transmision,
        fecha_folio,
        fecha,
        tipo_cirugia_ac,
        activo,
        hora_inicio_anes,
        hora_final_anes,
        hora_inicio_proce,
        hora_final_proce,
        estatus_bi,
        id_lugar_fisico,
        id_maquina,
        id_protesis,
        id_tipo_servicio,
        id_cirugia,
        id_especialidad_cir
    )
    VALUES %s
"""

def insertar_folios(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT (SQL Server)
        cur_origen.execute("""
            SELECT
                IdFolio,
                IdCliente,
                IdEntregaSBO,
                IdFacturaSBO,
                IdServicio,
                IdTerminal,
                TipoCirugiaUP,
                EstatusFolio,
                EstatusTransmision,
                FechaFolio,
                Fecha,
                TipoCirugiaAC,
                Activo,
                HoraInicioAnes,
                HoraFinalAnes,
                HoraInicioProce,
                HoraFinalProce,
                EstatusBI,
                IdLugarFisico,
                IdMaquina,
                IdProtesis,
                IdTipoServicio,
                IdCirugia,
                IdEspecialidadCir
            FROM dbo.Folios
            WHERE CreationDate >= '2026-01-01';
        """)

        folios_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(folios_origen)}")

        # ---------- DESTINO
        # Obtener los id_folio ya existentes
        cur_destino.execute("SELECT id_folio FROM fact_folios")
        folios_existentes = set(row[0] for row in cur_destino.fetchall())
        print(f"Registros destino: {len(folios_existentes)}")

        # ---------- TRANSFORM
        # Filtrar solo folios nuevos
        folios_dict = OrderedDict()

        for r in folios_origen:
            id_folio = r[0]
            if id_folio not in folios_existentes and id_folio not in folios_dict:
                folios_dict[id_folio] = (
                    r[0], r[1], r[2], r[3], r[4],
                    r[5], r[6], r[7], r[8], r[9],
                    r[10], r[11], r[12], r[13], r[14],
                    r[15], r[16], r[17], r[18], r[19],
                    r[20], r[21], r[22], r[23]
                )

        nuevos_folios = list(folios_dict.values())

        if not nuevos_folios:
            print("ℹ️ No hay nuevos folios para insertar.")
            return {
                "estatus": "success",
                "tabla": "fact_folios",
                "proceso": "insertar_folios",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # ---------- LOAD
        print(f"Insertando {len(nuevos_folios)} nuevos folios")
        execute_values(cur_destino, SQL_INSERT, nuevos_folios)
        conn_destino.commit()

        return {
            "estatus": "success",
            "tabla": "fact_folios",
            "proceso": "insertar_folios",
            "registros_insertados": len(nuevos_folios),
            "error_text": "No error"
        }

    except Exception:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "fact_folios",
            "proceso": "insertar_folios",
            "registros_insertados": 0,
            "error_text": traceback.format_exc()
        }