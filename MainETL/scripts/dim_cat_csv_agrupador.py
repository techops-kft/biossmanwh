import psycopg2
import csv
from psycopg2.extras import execute_values

def cargar_dim_cat_csv_agrupador():

    try:
        # ==========================================
        # Conexión a la base de datos
        # ==========================================
        conn = psycopg2.connect(
            host="aws-0-us-west-2.pooler.supabase.com",
            database="biossmanwh",
            user="postgres.ojworsbbrvoxhoarikjx",
            password="FbIpnkdaBlIaOaLu",
            port="5432"
        )

        cur = conn.cursor()

        # ==========================================
        # 1. Limpiar tabla destino
        # ==========================================
        print("🧹 Limpiando tabla dim_cat_csv_agrupador...")

        cur.execute("TRUNCATE TABLE dim_cat_csv_agrupador")

        # ==========================================
        # 2. Ruta del CSV
        # ==========================================
        ruta_csv = r"C:/Users/r_cmt/OneDrive/Escritorio/agrupadorqlik.csv"

        # ==========================================
        # 3. Leer CSV
        # ==========================================
        datos = []

        with open(ruta_csv, newline='', encoding="utf-8-sig") as csvfile:

            reader = csv.DictReader(csvfile)

            # limpiar nombres de columnas
            reader.fieldnames = [col.strip() for col in reader.fieldnames]

            print("Columnas detectadas:", reader.fieldnames)

            for row in reader:

                datos.append((
                    int(row.get("Consecutivo", 0)),
                    row.get("Agrupador"),
                    row.get("Material"),
                    row.get("Desc_Articulo_Inv"),
                    row.get("Des_Agrupador"),
                    row.get("Des_Concentrador")
                ))

        SQL_INSERT = """
        INSERT INTO dim_cat_csv_agrupador (
            id_consecutivo,
            id_agrupador,
            id_material,
            descripcion_material_inventario,
            descripcion_agrupador,
            descripcion_concentrador
        )
        VALUES %s
        """

        print("⬆️ Insertando registros...")

        execute_values(cur, SQL_INSERT, datos)

        conn.commit()

        print(f"✅ {len(datos)} registros insertados")

    except Exception as e:
        print("❌ Error durante la carga:")
        print(e)

    finally:

        if cur:
            cur.close()

        if conn:
            conn.close()

        print("🔌 Conexión cerrada")


cargar_dim_cat_csv_agrupador()