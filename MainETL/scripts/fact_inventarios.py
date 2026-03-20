import traceback
from psycopg2.extras import execute_values
from collections import OrderedDict

# ---------------------------------------------------------
# SQL INSERT
# ---------------------------------------------------------
SQL_INSERT = """
    INSERT INTO fact_inventarios (
        id_inventario,
        mesu_id,
        material_descripcion,
        matrl_grp_id,
        matrl_bar_cd,
        matrl_prvs,
        matrl_dt,
        indstry_stndrd,
        net_weight,
        trdmrk,
        load_dt,
        familia
    )
    VALUES %s
"""

def insertar_inventarios(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT
        cur_origen.execute("""                   
            SELECT 
                MATRL_NBR,
                PLNT_ID,
                WHSE_ID,
                INV_MATRL_BATCH,
                INV_MATRL_BATCH_DT,
                CMPNY_ID,
                BUSA_ID,
                MESU_ID,
                COST_TYPE_ID,
                CURNCY_ID,
                COST_UNIT,
                MATRL_TYPE_ID,
                GRP_PURCHS_ID,
                SPCL_STOCK_ID,
                INV_MATRL_STOCK_FUSE,
                INV_MATRL_STOCK_TRANST,
                INV_MATRL_STOCK_QUALTY,
                INV_MATRL_STOCK_NOTFUSE,
                INV_MATRL_STOCK_LOCKED,
                INV_MATRL_STOCK_CLAIM,
                STORAGE_TYPE,
                STOCK_CATEGORY,
                WHSE_WM_ID,
            FROM dbo.INVENTORIES;
        """)

        inventarios_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(inventarios_origen)}")

        # ---------- DESTINO
        cur_destino.execute("SELECT id_inventario FROM fact_inventarios")
        inventarios_existentes = set(row[0] for row in cur_destino.fetchall())

        print(f"Registros destino: {len(inventarios_existentes)}")

        # ---------- TRANSFORM
        inventarios_dict = OrderedDict()

        for r in inventarios_origen:

            id_inventario = r[0]

            if id_inventario not in inventarios_existentes and id_inventario not in inventarios_dict:

                inventarios_dict[id_inventario] = (
                    r[0],
                    r[1],
                    r[2],
                    r[3],
                    r[4],
                    r[5],
                    r[6],
                    r[7],
                    r[8],
                    r[9],
                    r[10]
                )

        nuevos_inventarios = list(inventarios_dict.values())

        if not nuevos_inventarios:

            print("ℹ️ No hay nuevos inventarios para insertar.")

            return {
                "estatus": "success",
                "tabla": "fact_inventarios",
                "proceso": "insertar_inventarios",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # ---------- LOAD
        print(f"Insertando {len(nuevos_inventarios)} nuevos inventarios")
        execute_values(cur_destino, SQL_INSERT, nuevos_inventarios)
        conn_destino.commit()

        return {
            "estatus": "success",
            "tabla": "fact_inventarios",
            "proceso": "insertar_inventarios",
            "registros_insertados": len(nuevos_inventarios),
            "error_text": "No error"
        }

    except Exception:

        conn_destino.rollback()

        return {
            "estatus": "failed",
            "tabla": "fact_inventarios",
            "proceso": "insertar_inventarios",
            "registros_insertados": 0,
            "error_text": traceback.format_exc()
        }