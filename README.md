Titulo Principal        # Proyecto final BigData

Descripción             # Ingestar registros CSV, TXT y SQL para validación de datos, proceso ETL y creación
                        de análisis, modelos, reglas y visualizaciones.

Arquitectura            # Diagrama DataLake

* **Bronze:** Ingesta de datos crudos y heterogéneos con validación básica de nulos.
* **Silver:** Limpieza de datos, normalización de texto, validar campos, manejar nulos, estandarización de fechas y unión (JOIN) de tablas para crear un esquema limpio y consistente.
* **Gold:** Capa de agregación final, lista para el análisis de negocio (tablas de resumen y reportes).
