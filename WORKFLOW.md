## ⚙️ Flujo de Trabajo (Workflow)

Este proyecto debe ejecutarse en el orden secuencial para garantizar la integridad de los datos entre las zonas Bronze, Silver y Gold.

### 1. Configuración del Entorno

1. **Clonar repositorio**
    ```bash
       git clone [https://github.com/osvaldogmc/BigData.git]
    ```
2. **Crear y activar entorno virtual**
    ```bash
    python -m venv venv
    Activar ambiente
    ```
3. **Instalar dependencias**
    ```bash
    pip install -r requirements.txt
    ```
4. **Configurar variables de entorno (hadoop/spark)**
    * Verificar que `JAVA_HOME` apunte a Java 17+.
    * Crear la variable `HADOOP_HOME` y añadir `%HADOOP_HOME%\bin` al `Path`.
    * Ejecutar el `chmod` de permisos (si es Windows).

### 2. Ejecución del Flujo ELT

Ejecutar los scripts de Python en el siguiente orden:

1.  **Ingesta a BRONZE (Lectura de Fuentes y Validación):**
    ```bash
    python ingesta_bronze.py
    ```
2.  **Limpieza y Transformación a SILVER (JOIN, Normalización, Fechas):**
    ```bash
    python silver_process.py
    ```
3.  **Análisis y Agregación desde silver (Reporte Final):**
    ```bash
    python 
    ```