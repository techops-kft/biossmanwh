import traceback
from psycopg2.extras import execute_values
from collections import OrderedDict

# 🔹 SQL INSERT (PostgreSQL)
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
        id_especialidad_cir,
        created_at
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
                IdEspecialidadCir,
                CreationDate
            FROM dbo.Folios
            WHERE CreationDate >= '2025-01-01';
        """)

        folios_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(folios_origen)}")

        # ---------- DESTINO
        cur_destino.execute("SELECT id_folio FROM fact_folios")
        folios_existentes = set(row[0] for row in cur_destino.fetchall())
        print(f"Registros destino: {len(folios_existentes)}")

        # ---------- TRANSFORM
        folios_dict = OrderedDict()

        for r in folios_origen:
            id_folio = r[0]
            if id_folio not in folios_existentes and id_folio not in folios_dict:
                folios_dict[id_folio] = (
                    r[0], r[1], r[2], r[3], r[4],
                    r[5], r[6], r[7], r[8], r[9],
                    r[10], r[11], r[12], r[13], r[14],
                    r[15], r[16], r[17], r[18], r[19],
                    r[20], r[21], r[22], r[23], r[24]
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



def actualizar_folios(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT DATA
        cur_origen.execute("""
            SELECT
                IdFolio,
                IdEntregaSBO,
                IdFacturaSBO,
                IdServicio,
                TipoCirugiaUP,
                EstatusFolio,
                EstatusTransmision,
                FechaFolio,
                Fecha,
                TipoCirugiaAC,
                HoraInicioAnes,
                HoraFinalAnes,
                HoraInicioProce,
                HoraFinalProce,
                IdLugarFisico,
                IdMaquina,
                IdTipoServicio,
                IdCirugia,
                IdEspecialidadCir
            FROM dbo.Folios
            WHERE CreationDate >= '2025-01-01';
        """)

        folios_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(folios_origen)}")

        cur_destino.execute("""
            SELECT
                id_folio,
                id_entrega_sbo,
                id_factura_sbo,
                id_servicio,
                tipo_cirugia_up,
                estatus_folio,
                estatus_transmision,
                fecha_folio,
                fecha,
                tipo_cirugia_ac,
                hora_inicio_anes,
                hora_final_anes,
                hora_inicio_proce,
                hora_final_proce,
                id_lugar_fisico,
                id_maquina,
                id_tipo_servicio,
                id_cirugia,
                id_especialidad_cir
            FROM fact_folios;
        """)

        folios_destino = cur_destino.fetchall()
        print(f"Registros destino: {len(folios_destino)}")

        # ---------- TRANSFORM
        mapa_destino = {
            f[0]: (
                f[1], f[2], f[3], f[4], f[5], f[6],
                f[7], f[8], f[9], f[10], f[11],
                f[12], f[13], f[14], f[15],
                f[16], f[17], f[18]
            )
            for f in folios_destino
        }

        actualizaciones = []

        for folio in folios_origen:

            id_folio = folio[0]

            valores_origen = (
                folio[1], folio[2], folio[3], folio[4],
                folio[5], folio[6], folio[7], folio[8],
                folio[9], folio[10], folio[11],
                folio[12], folio[13], folio[14],
                folio[15], folio[16], folio[17],
                folio[18]
            )

            datos_destino = mapa_destino.get(id_folio)

            if datos_destino:

                if datos_destino != valores_origen:

                    actualizaciones.append((
                        id_folio,
                        *valores_origen
                    ))

                    # ---------- DEBUG
                    print(f"Folio {id_folio} actualizado")

        # ---------- LOAD
        if actualizaciones:

            query = """
                WITH datos (
                    id_folio,
                    id_entrega_sbo,
                    id_factura_sbo,
                    id_servicio,
                    tipo_cirugia_up,
                    estatus_folio,
                    estatus_transmision,
                    fecha_folio,
                    fecha,
                    tipo_cirugia_ac,
                    hora_inicio_anes,
                    hora_final_anes,
                    hora_inicio_proce,
                    hora_final_proce,
                    id_lugar_fisico,
                    id_maquina,
                    id_tipo_servicio,
                    id_cirugia,
                    id_especialidad_cir
                ) AS (
                    VALUES %s
                )
                UPDATE fact_folios ff
                SET
                    id_entrega_sbo = datos.id_entrega_sbo,
                    id_factura_sbo = datos.id_factura_sbo,
                    id_servicio = datos.id_servicio,
                    tipo_cirugia_up = datos.tipo_cirugia_up,
                    estatus_folio = datos.estatus_folio,
                    estatus_transmision = datos.estatus_transmision,
                    fecha_folio = datos.fecha_folio,
                    fecha = datos.fecha,
                    tipo_cirugia_ac = datos.tipo_cirugia_ac,
                    hora_inicio_anes = datos.hora_inicio_anes,
                    hora_final_anes = datos.hora_final_anes,
                    hora_inicio_proce = datos.hora_inicio_proce,
                    hora_final_proce = datos.hora_final_proce,
                    id_lugar_fisico = datos.id_lugar_fisico,
                    id_maquina = datos.id_maquina,
                    id_tipo_servicio = datos.id_tipo_servicio,
                    id_cirugia = datos.id_cirugia,
                    id_especialidad_cir = datos.id_especialidad_cir
                FROM datos
                WHERE ff.id_folio = datos.id_folio;
            """

            execute_values(cur_destino, query, actualizaciones)
            conn_destino.commit()

            registros_actualizados = len(actualizaciones)
            print(f"Se actualizaron {registros_actualizados} folios.")

        else:
            print("No hubo folios por actualizar.")
            registros_actualizados = 0

        return {
            "estatus": "success",
            "tabla": "fact_folios",
            "proceso": "actualizar_folios",
            "registros_insertados": registros_actualizados,
            "error_text": "No error"
        }

    except Exception:
        conn_destino.rollback()

        return {
            "estatus": "failed",
            "tabla": "fact_folios",
            "proceso": "actualizar_folios",
            "registros_insertados": 0,
            "error_text": traceback.format_exc()
        }