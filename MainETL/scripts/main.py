import os
import pyodbc
import psycopg2
import importlib
import traceback
from dotenv import load_dotenv
import time

inicio = time.time()  # Marca de tiempo inicial

load_dotenv()

# Configuración de conexión a la base de datos origen
DB_ORIGEN_CONFIG = (
    f"DRIVER={{{os.getenv('SQLSERVER_DRIVER')}}};"
    f"SERVER={os.getenv('SQLSERVER_HOST')},{os.getenv('SQLSERVER_PORT')};"
    f"DATABASE={os.getenv('SQLSERVER_DB')};"
    f"UID={os.getenv('SQLSERVER_USER')};"
    f"PWD={os.getenv('SQLSERVER_PASSWORD')};"
    f"TrustServerCertificate={os.getenv('SQLSERVER_TRUST_CERT')};"
)

# Configuración de conexión a la base de datos destino
DB_DESTINO_CONFIG = {
    "dbname": f"{os.getenv('destino_dbname')}",
    "user": f"{os.getenv('destino_user')}",
    "password": f"{os.getenv('destino_password')}",
    "host": f"{os.getenv('destino_host')}",
    "port": f"{os.getenv('destino_port')}"
}


plan_ejecucion = [
    {"modulo": "dim_cat_cliente", "funcion": "insertar_cliente", "dependencia": [], "ejecucion": None},
    {"modulo": "dim_cat_cliente", "funcion": "actualizar_cliente", "dependencia": [], "ejecucion": None},
    {"modulo": "dim_cat_tipos_servicios", "funcion": "insertar_tipos_servicios", "dependencia": [], "ejecucion": None},
    {"modulo": "dim_cat_tipos_servicios", "funcion": "actualizar_tipos_servicios", "dependencia": [], "ejecucion": None},    
    {"modulo": "dim_cat_motivo_venta_no_realizada", "funcion": "insertar_motivo_venta_no_realizada", "dependencia": [], "ejecucion": None},
    {"modulo": "dim_cat_motivo_venta_no_realizada", "funcion": "actualizar_motivo_venta_no_realizada", "dependencia": [], "ejecucion": None},
    #{"modulo": "dim_cat_materiales", "funcion": "insertar_materiales", "dependencia": [], "ejecucion": None},
    {"modulo": "dim_cat_unidades_negocio", "funcion": "insertar_unidades_negocio", "dependencia": [], "ejecucion": None},
    {"modulo": "dim_cat_almacenes", "funcion": "insertar_almacenes", "dependencia": [], "ejecucion": None},
    {"modulo": "fact_folios", "funcion": "insertar_folios", "dependencia": [], "ejecucion": None},
    {"modulo": "fact_folios", "funcion": "actualizar_folios", "dependencia": [], "ejecucion": None},
    {"modulo": "fact_folios_intermedios", "funcion": "insertar_folios_intermedios", "dependencia": [], "ejecucion": None},
    {"modulo": "fact_folios_intermedios", "funcion": "actualizar_folios_intermedios", "dependencia": [], "ejecucion": None},
    {"modulo": "fact_articulos_venta_no_realizada_folios", "funcion": "insertar_articulos_venta_no_realizada_folios", "dependencia": [], "ejecucion": None},
    {"modulo": "fact_articulos_venta_no_realizada_folios", "funcion": "actualizar_articulos_venta_no_realizada_folios", "dependencia": [], "ejecucion": None},
    
]


try:
    conn_origen = pyodbc.connect(DB_ORIGEN_CONFIG)
    cur_origen = conn_origen.cursor()
    print("Conexión origen exitosa")

    conn_destino = psycopg2.connect(**DB_DESTINO_CONFIG)
    cur_destino = conn_destino.cursor()
    conn_destino.autocommit = False
    print("Conexión destino exitosa\n")

    # 🔁 BLOQUE CORREGIDO
    for step in plan_ejecucion:
        modulo = step["modulo"]
        funcion = step["funcion"]
        dependencias = step["dependencia"]

        print(f"Ejecutando: {modulo} -> {funcion}")

        # Verificar dependencias
        dependencias_fallidas = [
            dep for dep in dependencias
            if next((s for s in plan_ejecucion if s["modulo"] == dep), {}).get("ejecucion") != "success"
        ]

        if dependencias_fallidas:
            resultado = {
                "estatus": "failed",
                "tabla": modulo,
                "proceso": funcion,
                "registros_insertados": 0,
                "error_text": f"No se ejecutó por dependencia fallida: {', '.join(dependencias_fallidas)}"
            }
            step["ejecucion"] = "failed"

        else:
            try:
                mod = importlib.import_module(modulo)
                func = getattr(mod, funcion)
                resultado = func(cur_origen, conn_origen, cur_destino, conn_destino)
                step["ejecucion"] = resultado.get("estatus", "success")

            except Exception:
                resultado = {
                    "estatus": "failed",
                    "tabla": modulo,
                    "proceso": funcion,
                    "registros_insertados": 0,
                    "error_text": traceback.format_exc()
                }
                step["ejecucion"] = "failed"
                try:
                    conn_destino.rollback()
                except Exception:
                    pass

        # Registrar en migration_logs
        try:
            sql_log = """
                INSERT INTO migration_logs (
                    estatus,
                    tabla,
                    proceso,
                    registros_insertados,
                    error_text
                ) VALUES (%s, %s, %s, %s, %s)
            """
            cur_destino.execute(sql_log, (
                resultado["estatus"],
                resultado["tabla"],
                resultado["proceso"],
                resultado["registros_insertados"],
                resultado["error_text"]
            ))
            conn_destino.commit()
        except Exception as e_log:
            print(f"⚠️ Error al registrar log para {modulo}.{funcion}: {e_log}")
            try:
                conn_destino.rollback()
            except Exception:
                pass

        print(f"{'✅' if resultado['estatus'] == 'success' else '❌'} "
              f"{modulo}.{funcion}: {resultado['error_text']}\n")

except Exception as e:
    print(f"Error general: {e}")

finally:
    try:
        cur_origen.close()
        conn_origen.close()
    except Exception:
        pass
    try:
        cur_destino.close()
        conn_destino.close()
    except Exception:
        pass

    print("\nConexiones cerradas.")
    fin = time.time()
    print(f"Tiempo de ejecución total: {fin - inicio:.4f} segundos")