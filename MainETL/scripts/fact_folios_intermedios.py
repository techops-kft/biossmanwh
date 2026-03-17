import traceback
from psycopg2.extras import execute_values
from collections import OrderedDict

# 🔹 SQL INSERT (PostgreSQL)
SQL_INSERT = """
    INSERT INTO fact_folios_intermedios (
        id_folio,
        id_cliente,
        id_estatus,
        id_usuario,
        paciente_nombre,
        id_tipo_servicio,
        id_folio_padre,
        cirugia_programada_id,
        id_estatus_inicial,
        created_at
    )
    VALUES %s
"""

def insertar_folios_intermedios(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT (SQL Server)
        cur_origen.execute("""
            SELECT
                IdFolio,
                IdCliente,
                IdEstatus,
                IdUsuario,
                PacienteNombre,
                IdTipoServicio,
                IdFolioPadre,
                CirugiaProgramadaId,
                IdEstatusInicial,
                CreationDate
            FROM dbo.FoliosIntermedios
            WHERE CreationDate >= '2025-01-01';
        """)

        registros_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(registros_origen)}")

        # ---------- DESTINO
        cur_destino.execute("SELECT id_folio FROM fact_folios_intermedios")
        folios_existentes = set(row[0] for row in cur_destino.fetchall())
        print(f"Registros destino: {len(folios_existentes)}")

        # ---------- TRANSFORM
        folios_dict = OrderedDict()

        for r in registros_origen:
            idfolio = r[0]

            if idfolio not in folios_existentes and idfolio not in folios_dict:
                folios_dict[idfolio] = (
                    r[0],  # idfolio
                    r[1],  # idcliente
                    r[2],  # idestatus
                    r[3],  # idusuario
                    r[4],  # pacientenombre
                    r[5],  # idtiposervicio
                    r[6],  # idfoliopadre
                    r[7],  # cirugiaprogramadaid
                    r[8],  # idestatusinicial
                    r[9],  # CreationDate
                )

        nuevos_folios = list(folios_dict.values())

        if not nuevos_folios:
            print("ℹ️ No hay nuevos folios intermedios para insertar.")
            return {
                "estatus": "success",
                "tabla": "fact_folios_intermedios",
                "proceso": "insertar_folios_intermedios",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # ---------- LOAD
        print(f"Insertando {len(nuevos_folios)} nuevos registros")
        execute_values(cur_destino, SQL_INSERT, nuevos_folios)
        conn_destino.commit()

        return {
            "estatus": "success",
            "tabla": "fact_folios_intermedios",
            "proceso": "insertar_folios_intermedios",
            "registros_insertados": len(nuevos_folios),
            "error_text": "No error"
        }

    except Exception:
        conn_destino.rollback()
        return {
            "estatus": "failed",
            "tabla": "fact_folios_intermedios",
            "proceso": "insertar_folios_intermedios",
            "registros_insertados": 0,
            "error_text": traceback.format_exc()
        }



def actualizar_folios_intermedios(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT DATA
        cur_origen.execute("""
            SELECT
                IdFolio,
                IdEstatus,
                PacienteNombre,
                IdTipoServicio,
                IdFolioPadre
            FROM dbo.FoliosIntermedios
            WHERE CreationDate >= '2025-01-01';
        """)

        registros_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(registros_origen)}")

        cur_destino.execute("""
            SELECT
                id_folio,
                id_estatus,
                paciente_nombre,
                id_tipo_servicio,
                id_folio_padre
            FROM fact_folios_intermedios;
        """)

        registros_destino = cur_destino.fetchall()
        print(f"Registros destino: {len(registros_destino)}")

        # ---------- TRANSFORM
        mapa_destino = {
            r[0]: (r[1], r[2], r[3], r[4])
            for r in registros_destino
        }

        actualizaciones = []

        for registro in registros_origen:

            id_folio = registro[0]

            estatus_origen = registro[1]
            paciente_origen = registro[2]
            tipo_servicio_origen = registro[3]
            folio_padre_origen = registro[4]

            datos_destino = mapa_destino.get(id_folio)

            if datos_destino:

                estatus_destino, paciente_destino, tipo_servicio_destino, folio_padre_destino = datos_destino

                if (
                    estatus_destino != estatus_origen or
                    paciente_destino != paciente_origen or
                    tipo_servicio_destino != tipo_servicio_origen or
                    folio_padre_destino != folio_padre_origen
                ):

                    actualizaciones.append((
                        id_folio,
                        estatus_origen,
                        paciente_origen,
                        tipo_servicio_origen,
                        folio_padre_origen
                    ))

                    # ---------- DEBUG
                    print(
                        f"Folio {id_folio}: "
                        f"estatus {estatus_destino}→{estatus_origen}, "
                        f"paciente {paciente_destino}→{paciente_origen}, "
                        f"tipo_servicio {tipo_servicio_destino}→{tipo_servicio_origen}, "
                        f"folio_padre {folio_padre_destino}→{folio_padre_origen}"
                    )

        # ---------- LOAD
        if actualizaciones:

            query = """
                WITH datos (
                    id_folio,
                    id_estatus,
                    paciente_nombre,
                    id_tipo_servicio,
                    id_folio_padre
                ) AS (
                    VALUES %s
                )
                UPDATE fact_folios_intermedios ffi
                SET
                    id_estatus = datos.id_estatus,
                    paciente_nombre = datos.paciente_nombre,
                    id_tipo_servicio = datos.id_tipo_servicio,
                    id_folio_padre = datos.id_folio_padre
                FROM datos
                WHERE ffi.id_folio = datos.id_folio;
            """

            execute_values(cur_destino, query, actualizaciones)
            conn_destino.commit()

            registros_actualizados = len(actualizaciones)
            print(f"Se actualizaron {registros_actualizados} folios intermedios.")

        else:
            print("No hubo folios intermedios por actualizar.")
            registros_actualizados = 0


        return {
            "estatus": "success",
            "tabla": "fact_folios_intermedios",
            "proceso": "actualizar_folios_intermedios",
            "registros_insertados": registros_actualizados,
            "error_text": "No error"
        }

    except Exception:
        conn_destino.rollback()

        return {
            "estatus": "failed",
            "tabla": "fact_folios_intermedios",
            "proceso": "actualizar_folios_intermedios",
            "registros_insertados": 0,
            "error_text": traceback.format_exc()
        }