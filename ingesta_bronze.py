import pandas as pd
import os
import sqlite3
import re
from glob import glob

BASE = "DATALAKE"
BRONZE_PATH = os.path.join(BASE, "bronze", "ventas")
os.makedirs(BRONZE_PATH, exist_ok=True)

rutas = [
    "lidl_project/clientes_info.csv",
    "lidl_project/clientes_extra.txt",
    "lidl_project/clientes.sql"
]

def validar(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(how="all")
    df = df.fillna("NULL")
    return df

def read_csv_like(path):
    try:
        df = pd.read_csv(path)
        return df
    except Exception:
        try:
            df = pd.read_csv(path, sep="\t")
            return df
        except Exception as e:
            raise

def read_sql_via_sqlite(sql_path):
    with open(sql_path, "r", encoding="utf-8", errors="ignore") as f:
        sqltext = f.read()

    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()

    try:
        cur.executescript(sqltext)
        conn.commit()
    except Exception as e:
        conn.close()
        raise
     
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [r[0] for r in cur.fetchall()]
    if not tables:
        conn.close()
        raise RuntimeError("No se detectaron tablas tras ejecutar SQL en sqlite.")
    # leer la primera tabla (o podrías iterar todas)
    df = pd.read_sql_query(f"SELECT * FROM {tables[0]}", conn)
    conn.close()
    return df

def read_sql_fallback_parse(sql_path):
    """Fallback: parsear INSERT INTO ... VALUES(...) y construir DataFrame"""
    with open(sql_path, "r", encoding="utf-8", errors="ignore") as f:
        text = f.read()

    # buscar un primer INSERT INTO ... (col1,col2,...) VALUES (....);
    m = re.search(r"INSERT\s+INTO\s+[`\"]?(\w+)[`\"]?\s*\(([^)]+)\)\s*VALUES\s*(.+);", text, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        # intentar encontrar sólo VALUES(....) secuencias
        inserts = re.findall(r"VALUES\s*\((.*?)\)\s*[,;]", text, flags=re.IGNORECASE | re.DOTALL)
        if not inserts:
            raise RuntimeError("No se detectaron INSERT/VALUES en el .sql para parsear.")
        # sin columnas, creamos columnas numeradas
        rows = []
        for ins in inserts:
            # dividir por comas respetando comillas simples/dobles básicas
            parts = [p.strip().strip("'\"") for p in re.split(r",(?=(?:[^']*'[^']*')*[^']*$)", ins)]
            rows.append(parts)
        maxcols = max(len(r) for r in rows)
        cols = [f"col_{i+1}" for i in range(maxcols)]
        df = pd.DataFrame(rows, columns=cols)
        return df

    # si se encontró la estructura con columnas
    cols_raw = m.group(2)
    cols = [c.strip().strip("`\"'") for c in cols_raw.split(",")]
    values_section = m.group(3)

    # extraer todas las tuplas VALUES (v1, v2), (v1, v2), ...
    tuples = re.findall(r"\((.*?)\)(?:\s*,\s*|\s*;)", values_section, flags=re.DOTALL)
    rows = []
    for t in tuples:
        parts = [p.strip().strip("'\"") for p in re.split(r",(?=(?:[^']*'[^']*')*[^']*$)", t)]
        rows.append(parts)
    df = pd.DataFrame(rows, columns=cols[:len(rows[0])])
    return df

# --- proceso principal ---
for ruta in rutas:
    # ... (código previo) ...

    ext = os.path.splitext(ruta)[1].lower()
    try:
        if ext in [".csv"]:
            df = read_csv_like(ruta)

        elif ext in [".txt"]:
            print(" -> Procesando TXT: Asumiendo separador coma y sin cabecera...")
            
            # Forzamos lectura con separador coma y sin cabecera (header=None)
            df = pd.read_csv(ruta, sep=",", header=None)
            
            # Asignamos nombres claros para el JOIN futuro
            df.columns = ["codigo", "origen_compra", "id_usuario", "fecha_registro"]

        elif ext in [".sql"]:
            # primero intento con sqlite
            try:
                df = read_sql_via_sqlite(ruta)
            except Exception as e:
                print("  sqlite falló:", e)
                print("  intentando parse fallback de INSERTs...")
                df = read_sql_fallback_parse(ruta)

        else:
            # intento lectura genérica con pandas
            df = read_csv_like(ruta)

        # validaciones basicas
        df = validar(df)

        # generar nombre destino (Método seguro para garantizar formato _bronze.csv)
        nombre_base = os.path.splitext(os.path.basename(ruta))[0]
        nombre_dest = f"{nombre_base}_bronze.csv"

        destino = os.path.join(BRONZE_PATH, nombre_dest)
        df.to_csv(destino, index=False)
        print("  -> Guardado en:", destino)
    except Exception as e:
        print("  ERROR procesando", ruta, ":", e)

print("Carga BRONZE completada.")