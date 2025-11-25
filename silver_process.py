from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, trim, regexp_replace, to_date, when, expr
)
import os

# ----------------------------------------
# RUTAS CONFIGURADAS
# ----------------------------------------
# Asegúrate que el DATALAKE existe y la carpeta bronze/ventas tiene los archivos
BASE = "C:/Users/osvaldo.morales/Documents/Inacap/bigdata/DataLake/DATALAKE"

BRONZE_PATH = BASE + "/bronze/ventas/"
SILVER_PATH = BASE + "/silver/ventas/"

# Crear carpeta silver si falta
os.makedirs(SILVER_PATH, exist_ok=True)

# Spark necesita rutas tipo file:///C:/...
bronze_uri = "file:///" + BRONZE_PATH.replace("\\", "/")
silver_uri = "file:///" + SILVER_PATH.replace("\\", "/")


# ----------------------------------------
# INICIAR SPARK
# ----------------------------------------
spark = SparkSession.builder \
    .appName("SilverProcess") \
    .config("spark.hadoop.validateOutputSpecs", "false") \
    .getOrCreate()


# ----------------------------------------
# 1. LEER BRONZE POR ARCHIVO Y UNIR
# ----------------------------------------
print("Leyendo y uniendo archivos desde BRONZE...")

# A. Leer tabla principal (clientes_bronze)
# La llamaremos df_1. Contiene la fecha_nacimiento
df_1 = spark.read.csv(
    bronze_uri + "clientes_bronze.csv",
    header=True,
    inferSchema=True
)

# B. Leer tabla de info (clientes_info_bronze)
# La llamaremos df_2. Contiene el dato numérico '15'
df_2 = spark.read.csv(
    bronze_uri + "clientes_info_bronze.csv",
    header=True,
    inferSchema=True
)

# C. Unir las dos tablas
# Primero, normalizamos el nombre de la columna de unión
df_2 = df_2.withColumnRenamed("codigo_cliente", "codigo")

# Unir por la clave 'codigo'
df = df_1.join(df_2, on="codigo", how="inner")

# Nota: El archivo clientes_extra_bronze.csv se omite porque está corrupto (1 columna).
# Debería ser reprocesado en la etapa de ingesta.

print("BRONZE cargado y unido correctamente.")
df.printSchema()


# ----------------------------------------
# 2. NORMALIZACIÓN TEXTUAL
# ----------------------------------------
# ... (El código de normalización sigue igual y es correcto)
for c in df.columns:
    df = df.withColumn(c, regexp_replace(col(c), r"[\r\n\t]", ""))
    df = df.withColumn(c, trim(col(c)))
    df = df.withColumn(c, lower(col(c)))


# ----------------------------------------
# 3. DETECTAR COLUMNAS FECHA
# ----------------------------------------
# La única columna de fecha es 'fecha_nacimiento'
columnas_fecha = ['fecha_nacimiento'] 

print("Columnas detectadas como fecha:", columnas_fecha)

# ----------------------------------------
# 4. PARSE SEGURO DE FECHAS (Usando to_date, que ya devuelve NULL si falla)
# ----------------------------------------
for c in columnas_fecha:
    # Usamos to_date en lugar de try_to_date (para compatibilidad)
    # y ya maneja el error convirtiendo el valor a NULL si no es YYYY-MM-DD
    df = df.withColumn(c, to_date(col(c), 'yyyy-MM-dd'))


# ----------------------------------------
# 5. MANEJO DE NULOS
# ----------------------------------------
# Ahora, la columna 'tiempo_permanencia_min' ya no será interpretada como fecha
for c in df.columns:

    dtype = str(df.schema[c].dataType)

    # Si es de tipo DateType (la columna 'fecha_nacimiento' ya lo será si fue exitosa)
    if "DateType" in dtype:
        continue

    # Si es StringType, reemplazar nulo con "desconocido"
    elif "StringType" in dtype:
        df = df.withColumn(c, when(col(c).isNull(), "desconocido").otherwise(col(c)))

    # Si es numérica (ya no entrará 'tiempo_permanencia_min' con error de fecha)
    else:
        df = df.withColumn(c, when(col(c).isNull(), 0).otherwise(col(c)))


# ----------------------------------------
# 6. GUARDAR EN SILVER
# ----------------------------------------
print("Guardando en:", silver_uri)

df.write.mode("overwrite").option("header", True).csv(silver_uri)

print("Datos SILVER guardados en", SILVER_PATH)

spark.stop()