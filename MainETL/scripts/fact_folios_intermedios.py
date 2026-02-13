import traceback
from psycopg2.extras import execute_values
from collections import OrderedDict

# 🔹 SQL de inserción (PostgreSQL)
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
        id_estatus_inicial
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
                IdEstatusInicial
            FROM dbo.FoliosIntermedios
            WHERE CreationDate >= '2026-01-01';
        """)

        registros_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(registros_origen)}")

        # ---------- DESTINO
        # Obtener id_folio ya existentes
        cur_destino.execute("SELECT id_folio FROM fact_folios_intermedios")
        folios_existentes = set(row[0] for row in cur_destino.fetchall())
        print(f"Registros destino: {len(folios_existentes)}")

        # ---------- TRANSFORM
        # Filtrar solo nuevos y deduplicar
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