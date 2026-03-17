import traceback
from psycopg2.extras import execute_values
from collections import OrderedDict

# ---------------------------------------------------------
# Función para determinar la familia del material
# ---------------------------------------------------------
def obtener_familia(id_articulo):

    if id_articulo is None:
        return "Sin Definir"

    cod3 = id_articulo[2:4] if len(id_articulo) >= 4 else ""
    cod6 = id_articulo[5:7] if len(id_articulo) >= 7 else ""

    if cod3 == 'D2' and cod6 == '01':
        return 'Consumibles y Servicios (ENDO)'
    elif cod3 == 'T2' and cod6 == '01':
        return 'Consumibles y Servicios (OSTEO)'
    elif cod6 == '02':
        return 'Implantes Rodilla'
    elif cod6 == '03':
        return 'Implantes Cadera'
    elif cod6 == '04':
        return 'Hemiprótesis De Cadera'
    elif cod6 == '05':
        return 'Tornillos Dhs'
    elif cod6 == '06':
        return 'Ccm Femur'
    elif cod6 == '07':
        return 'Ccm Tibia'
    elif cod6 == '08':
        return 'Ccm Húmero'
    elif cod6 == '09':
        return 'Canulados 4.0'
    elif cod6 == '10':
        return 'Canulados 6.5 Y 7.0'
    elif cod6 == '11':
        return 'Cerclaje'
    elif cod6 == '12':
        return 'Kirschner-Steinmann'
    elif cod6 == '13':
        return 'Fijadores'
    elif cod6 == '14':
        return 'Placas Dhs'
    elif cod6 == '15':
        return 'Placas Convencional'
    elif cod6 == '16':
        return 'Tornillos Corticales Convencional'
    elif cod6 == '17':
        return 'Tornillos Esponja Convencional'
    elif cod6 == '18':
        return 'Placas Bloqueadas'
    elif cod6 == '19':
        return 'Tornillos Corticales Bloqueados'
    elif cod6 == '20':
        return 'Tornillos Esponja Bloqueados'
    elif cod6 == '21':
        return 'Columna Cervical'
    elif cod6 == '22':
        return 'Columna Lumbar'
    elif cod6 == '23':
        return 'Columna Terceros'
    elif cod6 == '24':
        return 'Ortopedia Blanda'
    elif cod6 == '25':
        return 'Placas Maxilo'
    elif cod6 == '26':
        return 'Tornillos Maxilo'
    elif cod6 == '27':
        return 'Instrumentales SIO'
    elif cod6 == '40':
        return 'Accesorios De Aspiración Y Succión'
    elif cod6 == '41':
        return 'Accesorios De Especialidad'
    elif cod6 == '42':
        return 'Accesorios De Laringoscopio Y Videolaringoscopio'
    elif cod6 == '43':
        return 'Accesorios De Ventilación'
    elif cod6 == '44':
        return 'Accesorios Quirurgicos'
    elif cod6 == '45':
        return 'Accesorios Vasculares'
    elif cod6 == '46':
        return 'Aguja Hipodermica'
    elif cod6 == '47':
        return 'Aguja Neuroestimulación'
    elif cod6 == '48':
        return 'Agujas Para Biopsia Y Transplante'
    elif cod6 == '49':
        return 'Agujas Y Equipos De Bloqueo'
    elif cod6 == '50' and cod3 == 'M5':
        return 'Sistemas De Fijacion'
    elif cod6 == '50' and cod3 == 'M7':
        return 'Manufactura Sistemas De Fijacion'
    elif cod6 == '51':
        return 'Apositos Y Cuidado De Heridas'
    elif cod6 == '52':
        return 'Cal'
    elif cod6 == '53':
        return 'Cateteres Y Accesorios'
    elif cod6 == '54' and cod3 == 'M5':
        return 'Cintas Y Adhesivos'
    elif cod6 == '54' and cod3 == 'M7':
        return 'Manufactura Cintas Y Adhesivos'
    elif cod6 == '55':
        return 'Circuitos De Ventilación'
    elif cod6 == '56':
        return 'Cotonoides, Compresas Y Gasas'
    elif cod6 == '57' and cod3 == 'M5':
        return 'Cubrebocas'
    elif cod6 == '57' and cod3 == 'M7':
        return 'Manufactura Cubrebocas'
    elif cod6 == '58':
        return 'Dialisis Y Hemodialisis'
    elif cod6 == '59':
        return 'Equipos Para Venoclisis E Infusión'
    elif cod6 == '60' and cod3 == 'M5':
        return 'Material De Empaque Y Esterilización'
    elif cod6 == '60' and cod3 == 'M7':
        return 'Manufact. Material De Empaque Y Esterilización'
    elif cod6 == '61':
        return 'Geles Y Lubricantes'
    elif cod6 == '62':
        return 'Guantes'
    elif cod6 == '63':
        return 'Instrumental Quirúrgico'
    elif cod6 == '64':
        return 'Insumos De Odontología'
    elif cod6 == '65':
        return 'Jeringas'
    elif cod6 == '66':
        return 'Kits'
    elif cod6 == '67':
        return 'Mascarillas'
    elif cod6 == '68':
        return 'Mascarillas Laringeas'
    elif cod6 == '69':
        return 'Medias Y Fundas'
    elif cod6 == '70':
        return 'Paquetes Quirurgicos'
    elif cod6 == '71' and cod3 == 'M5':
        return 'Plasticos'
    elif cod6 == '71' and cod3 == 'M7':
        return 'Manufactura Plasticos'
    elif cod6 == '72':
        return 'Recoleccion De Muestras'
    elif cod6 == '73' and cod3 == 'M5':
        return 'Ropa Quirúrgica'
    elif cod6 == '73' and cod3 == 'M7':
        return 'Manufactura Ropa Quirúrgica'
    elif cod6 == '74':
        return 'Sensores, Electrodos Y Transductores'
    elif cod6 == '75':
        return 'Sondas'
    elif cod6 == '76':
        return 'Suturas'
    elif cod6 == '77':
        return 'Tubos Endotraqueales Y Endobronqueales'
    elif cod6 == '78':
        return 'Vendas'
    elif cod6 == '79':
        return 'SERVICIOS'
    elif cod6 == '80':
        return 'Manufactura Etiquetas Y Caratulas'
    elif cod6 == '81':
        return 'Manufactura Telas Y Rollos'
    elif cod6 == '87':
        return 'Antisépticos'
    elif cod6 == '90':
        return 'Accesorios Shimadzu'
    elif cod6 == '91':
        return 'Consumibles Shimadzu'
    elif cod6 == '92':
        return 'Equipos De Imagen Shimadzu'
    elif cod6 == '93':
        return 'Refacciones Shimadzu'
    else:
        return 'Sin Definir'


# ---------------------------------------------------------
# SQL INSERT
# ---------------------------------------------------------
SQL_INSERT = """
    INSERT INTO dim_cat_materiales (
        id_material,
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

def insertar_materiales(cur_origen, conn_origen, cur_destino, conn_destino):
    try:
        # ---------- EXTRACT
        cur_origen.execute("""
            SELECT
                MATRL_NBR,
                MESU_ID,
                MATRL_DESC,
                MATRL_GRP_ID,
                MATRL_BAR_CD,
                MATRL_PRVS,
                MATRL_DT,
                INDSTRY_STNDRD,
                NET_WEIGHT,
                TRDMRK,
                LOAD_DT
            FROM dbo.MATERIALS;
        """)

        materiales_origen = cur_origen.fetchall()
        print(f"Registros origen: {len(materiales_origen)}")

        # ---------- DESTINO
        cur_destino.execute("SELECT id_material FROM dim_cat_materiales")
        materiales_existentes = set(row[0] for row in cur_destino.fetchall())

        print(f"Registros destino: {len(materiales_existentes)}")

        # ---------- TRANSFORM
        materiales_dict = OrderedDict()

        for r in materiales_origen:

            id_material = r[0]

            if id_material not in materiales_existentes and id_material not in materiales_dict:

                familia = obtener_familia(id_material)

                materiales_dict[id_material] = (
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
                    r[10],
                    familia
                )

        nuevos_materiales = list(materiales_dict.values())

        if not nuevos_materiales:

            print("ℹ️ No hay nuevos materiales para insertar.")

            return {
                "estatus": "success",
                "tabla": "dim_cat_materiales",
                "proceso": "insertar_materiales",
                "registros_insertados": 0,
                "error_text": "No error"
            }

        # ---------- LOAD
        print(f"Insertando {len(nuevos_materiales)} nuevos materiales")
        execute_values(cur_destino, SQL_INSERT, nuevos_materiales)
        conn_destino.commit()

        return {
            "estatus": "success",
            "tabla": "dim_cat_materiales",
            "proceso": "insertar_materiales",
            "registros_insertados": len(nuevos_materiales),
            "error_text": "No error"
        }

    except Exception:

        conn_destino.rollback()

        return {
            "estatus": "failed",
            "tabla": "dim_cat_materiales",
            "proceso": "insertar_materiales",
            "registros_insertados": 0,
            "error_text": traceback.format_exc()
        }