# SI7006-SI6003_Trabajo2

**Trabajo 2 de Almacenamiento y Procesamiento de Grandes Datos — 2026**

> Alejandro Quintero Moreno  
> Santiago Gomez Rueda
# 0. Introducción
La documentacion presentada tiene el proposito de explicar el procedimiento paso paso sobre para implementar un pipeline batch con Apache Spark en AWS y un flujo de Streaming con Amazon Kinesis Data Streams.
# 1. Parte A
## 1.1 Datos de Partida
Estaremos trabajando con los datos del BTS (Bureau of Transportation Statistics) para el Dataset principal. Para el Dataset secundario se utilizaran los datos de OurAirports, los cuales contienen informacion de Aeropuertos tal como:
-   Código IATA del aeropuerto.    
-   Nombre del aeropuerto.
-   Ciudad y país.
-   Coordenadas geográficas.
-   Tipo de aeropuerto.

En nuestro Bucket S3 de AWS creamos la carpeta input de la siguiente forma:

    input/ 
    ├── flights/
    │   └── bts_bronze.csv
    └── airports/
        └── airports.csv        
En la carpeta flights subimos los datos bronce de los vueltos, mientras que en la carpeta airports el Dataset de aeropuertos.
## 1.2 Crawlers iniciales en AWS Glue
Con el objetivo de encontrar los esquemas de bts_bronze.csv y airports.csv creamos un crawler para cada archivo.
Primero, en el buscador de AWS escribimos AWS Glue. 

> Para mantener la consistencia de roles durante todo el trabajo
> evitaremos crear roles ya que los permisos de AWS Academy no
> contemplan esa opcion.


1. En el panel lateral izquierdo, nos desplazamos hacia la seccion Data Catalog, al desplegarlo le daremos click a la seccion DataBases, alli verificaremos si tenemos un DataBase asignado para el trabajo 2, de no ser el caso, creamos un database con el nombre de trabajo2.
2. En el panel lateral, nos dirigimos hacia la seccion de Crawlers. Estando en el panel creamos nuestro primer Crawler con la siguiente configuracion:
Nombre: `crawler_bts_flights`
IAM role: `LabRole` (Seleccionado de un rol ya existente)
Database: `trabajo2`
Data Source: `rutas3/inputs/flights` (Adaptarlo al nombre del Bucket S3)
Frequency: `On demand`

Creamos el crawler y lo ejecutamos, en aproximadamente un minuto el Crawler habra reconocido el esquema. Esto lo podemos verificar si en el panel lateral, debajo de Data Catalog, dentro de la opcion Tables elegimos la database trabajo2.Luego de esto podremos ver en la lista de tablas disponibles la tabla `flights`. La tabla se puede revisar en Athena y verificar su contenido mediante consultas SQL.

3. Repetimos este proceso para generar un esquema para los aeropuertos.
Nombre: `crawler_airports`
IAM role: `LabRole` 
Target Database: `trabajo2`
Data Source: `rutas3/inputs/airports`
Frequency: `On demand`

Verificamos el esquema generado por el crawler en Athena. En ambos casos se deberian generar tablas accesibles y con columnas identificadas. Esto es relevante para planear como hacer el Join.

## 1.3 AWS Glue ETL y primer Job

Según el flujo planteado para el trabajo definiremos tres jobs.

- Job 1 Limpieza de vuelos, JOIN con aeropuertos y escritura en Parquet particionado.
- Job 2 Analisis a la pregunta: ¿Qué aerolíneas presentan el mayor porcentaje de vuelos retrasados y cuál es su retraso promedio?
- Job 3 Agregación para la pregunta: ¿Qué aeropuertos de origen presentan la mayor proporción de vuelos retrasados?

El objetivo del Job 1 es generar datos particionados a partir de la union de la informacion de los vuelos y la informacion de los aeropuertos, de esta forma enriqueciendo los datos mediante el uso de una columna en comun.
demás, tenemos información geográfica que enriquecerá los registros.

### 1.3.1 Explicacion del JOIN

El dataset BTS tiene dos columnas importantes:

-   `Origin`: código IATA del aeropuerto de salida.
    
-   `Dest`: código IATA del aeropuerto de llegada.
    

El dataset de aeropuertos tiene `iata_code`.

Por tanto, haremos dos JOIN independientes:

 **Primer JOIN: aeropuerto de origen**

`flights.Origin = airports.iata_code`

Columnas resultantes:

-   `Origin_airport_name`
    
-   `Origin_city`
    
-   `Origin_country`
    
-   `Origin_latitude`
    
-   `Origin_longitude`
    
   **Segundo JOIN: aeropuerto de destino**
    

`flights.Dest = airports.iata_code`

Columnas resultantes:

-   `Dest_airport_name`
    
-   `Dest_city`
    
-   `Dest_country`
    
-   `Dest_latitude`
    
-   `Dest_longitude`

### 1.3.2 Flujo de procesamiento
El procesamiento será:

1.  Leer `flights` desde S3.
    
2.  Leer `airports` desde S3.
    
3.  Eliminar registros sin `year`, `origin`, `dest` o `reporting_airline`.
    
4.  Descartar registros duplicados.
    
5.  Validar los códigos IATA.
    
6.  Realizar dos LEFT JOIN con aeropuertos.
    
7.  Guardar el resultado como Parquet particionado por `year`.

## 1.4 Configuracion del Job 1
###  1. Abrir el editor de AWS Glue

1.  Entramos a AWS Glue.
2.  En el menú izquierdo, seleccionamos ETL jobs.
3.  Pulsar la opcion Script editor.
4.  Seleccionar Spark como motor de procesamiento.
5.  Seleccionar Python como lenguaje.

### 2. Dentro de la pantalla de Job Details definimos:
Job name: `job1_flights_clean_join`
IAM Role: `LabRole`
Glue version: `Predeterminada`
Language: `Python`
Worker type: `G.1X`
Number of workers: `2`

Dentro del editor de codigo de PySpark agregaremos el siguiente codigo.
    

    import sys
    
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    
    from pyspark.context import SparkContext
    from pyspark.sql import functions as F
    
    # ============================================================
    # 1. INICIALIZAR GLUE Y SPARK
    # ============================================================
    
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    # ============================================================
    # 2. DEFINIR RUTAS
    # ============================================================
    
    FLIGHTS_PATH = "s3://almacen-bts-trabajo2/input/flights/bts_bronze.csv"
    AIRPORTS_PATH = "s3://almacen-bts-trabajo2/input/airports/airports.csv"
    
    OUTPUT_PATH = "s3://almacen-bts-trabajo2/output/flights_enriched/"
    
    # ============================================================
    # 3. LEER DATASETS
    # ============================================================
    
    # Leer vuelos BTS
    flights = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(FLIGHTS_PATH)
    )
    
    # Leer aeropuertos
    airports = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(AIRPORTS_PATH)
    )
    
    print("Columnas de vuelos:", flights.columns)
    print("Columnas de aeropuertos:", airports.columns)
    
    print("Total de vuelos:", flights.count())
    print("Total de aeropuertos:", airports.count())
    
    # ============================================================
    # 4. NORMALIZAR COLUMNAS DE IDENTIFICACIÓN
    # ============================================================
    
    flights = (
        flights
        .withColumn("origin", F.upper(F.trim(F.col("origin"))))
        .withColumn("dest", F.upper(F.trim(F.col("dest"))))
        .withColumn(
            "reporting_airline",
            F.upper(F.trim(F.col("reporting_airline")))
        )
    )
    
    # ============================================================
    # 5. LIMPIAR DATASET DE VUELOS
    # ============================================================
    
    flights_clean = flights.filter(
        F.col("year").isNotNull() &
        F.col("origin").isNotNull() &
        (F.col("origin") != "") &
        F.col("dest").isNotNull() &
        (F.col("dest") != "") &
        F.col("reporting_airline").isNotNull() &
        (F.col("reporting_airline") != "") &
        F.col("distance").isNotNull() &
        (F.col("distance") > 0)
    )
    
    # Eliminar registros completamente duplicados
    flights_clean = flights_clean.dropDuplicates()
    
    original_count = flights.count()
    clean_count = flights_clean.count()
    
    print("Vuelos originales:", original_count)
    print("Vuelos después de limpieza:", clean_count)
    print("Registros descartados:", original_count - clean_count)
    
    # ============================================================
    # 6. LIMPIAR DATASET DE AEROPUERTOS
    # ============================================================
    
    airports_clean = (
        airports
        .select(
            F.upper(F.trim(F.col("iata_code"))).alias("iata_code"),
            F.col("name").alias("airport_name"),
            F.col("municipality").alias("city"),
            F.col("iso_country").alias("country"),
            F.col("latitude_deg").alias("latitude"),
            F.col("longitude_deg").alias("longitude")
        )
        .filter(
            F.col("iata_code").isNotNull() &
            (F.col("iata_code") != "")
        )
        .dropDuplicates(["iata_code"])
    )
    
    print("Total de aeropuertos después de limpieza:", airports_clean.count())
    
    # ============================================================
    # 7. PREPARAR AEROPUERTOS DE ORIGEN
    # ============================================================
    
    origin_airports = airports_clean.select(
        F.col("iata_code").alias("origin_iata"),
        F.col("airport_name").alias("origin_airport_name"),
        F.col("city").alias("origin_city"),
        F.col("country").alias("origin_country"),
        F.col("latitude").alias("origin_latitude"),
        F.col("longitude").alias("origin_longitude")
    )
    
    # ============================================================
    # 8. JOIN CON AEROPUERTOS DE ORIGEN
    # ============================================================
    
    flights_enriched = (
        flights_clean
        .join(
            origin_airports,
            flights_clean.origin == origin_airports.origin_iata,
            "left"
        )
        .drop("origin_iata")
    )
    
    # ============================================================
    # 9. PREPARAR AEROPUERTOS DE DESTINO
    # ============================================================
    
    dest_airports = airports_clean.select(
        F.col("iata_code").alias("dest_iata"),
        F.col("airport_name").alias("dest_airport_name"),
        F.col("city").alias("dest_city"),
        F.col("country").alias("dest_country"),
        F.col("latitude").alias("dest_latitude"),
        F.col("longitude").alias("dest_longitude")
    )
    
    # ============================================================
    # 10. JOIN CON AEROPUERTOS DE DESTINO
    # ============================================================
    
    flights_enriched = (
        flights_enriched
        .join(
            dest_airports,
            flights_enriched.dest == dest_airports.dest_iata,
            "left"
        )
        .drop("dest_iata")
    )
    
    # ============================================================
    # 11. VERIFICAR DATASET ENRIQUECIDO
    # ============================================================
    
    print("Columnas del dataset enriquecido:")
    print(flights_enriched.columns)
    
    enriched_count = flights_enriched.count()
    
    print("Total de vuelos después de los JOIN:", enriched_count)
    
    # Mostrar ejemplos del resultado
    flights_enriched.select(
        "origin",
        "origin_airport_name",
        "origin_city",
        "dest",
        "dest_airport_name",
        "dest_city"
    ).show(10, truncate=False)
    
    # ============================================================
    # 12. GUARDAR RESULTADO EN PARQUET
    # ============================================================
    
    (
        flights_enriched
        .write
        .mode("overwrite")
        .partitionBy("year")
        .parquet(OUTPUT_PATH)
    )
    
    print("Dataset enriquecido guardado correctamente.")
    print("Ruta de salida:", OUTPUT_PATH)
    
    # ============================================================
    # 13. FINALIZAR JOB
    # ============================================================
    
    job.commit()
    
    print("AWS Glue Job finalizado correctamente.")

Luego de tener el código listo, guardamos el script y nos aseguramos de que no haya ningún error de compilación. A continuación podemos ejecutar el Job. Teniendo en cuenta el volumen de los datos (Aproximadamente 4.5 GB en csv) puede extenderse hasta 20 minutos. 
## 1.5 Resultados
### 1.5.1 Resultados de limpieza

**Vuelos originales** 32.386.436
**Vuelos después de limpieza** 32.386.436

**Registros descartados** 0

**Aeropuertos originales** 86.336

**Aeropuertos después de limpieza** 9.053

### 1.5.2 Resultados del JOIN
**Vuelos antes de los JOIN** 32.386.436
**Vuelos después de los JOIN** 32.386.436

### 1.5.3 Revision de los resultados
Para este trabajo el objetivo es el enriquecimiento de los datos, en el Trabajo 1 la limpieza de los datos solo elimino registros registros cuyo valor en la variable objetivo era nulo. Para esta composicion de datos la condicion de la variable objetivo no justificaba aun la eliminacion de dichos registros, ya que se estan analizando los destinos y origenes. Por lo tanto, la limpieza de registros duplicados o de campos vacios no arrojo ningun registro que necesitara eliminarse. 

Lo anterior reafirma la calidad de los datos, coherente con ser obtenidos de una entidad que lleva un registro estricto de los mismos. Despues del Join no se generaron nuevos vuelos ya que lo que se hace realmente es agregar columnas con informacion de aeropuertos, origenes y destinos. Cumpliendo asi con el enriquecimiento planteado de los datos.

El resultado contiene 44 columnas:

-   35 columnas originales del dataset BTS.
    
-   9 columnas nuevas relacionadas con los aeropuertos.
    

Las columnas nuevas son:

- `Origen`

- `Destino`

- `origin_airport_name`

- `dest_airport_name`

- `origin_city`

- `dest_city`

- `origin_country`

- `dest_country`

- `origin_latitude`

- `dest_latitude`

- `origin_longitude`

- `dest_longitude`
## 1.6 Jobs 2 y 3
### 1.6.1 Job 2
El segundo Job utiliza como entrada el dataset enriquecido en formato  
Parquet generado por `job1_flights_clean_join`. Su propósito es calcular  
indicadores de retraso agrupados por año y por aerolínea.

La pregunta que responde esta agregación es:

**¿Qué porcentaje de vuelos presenta retrasos para cada aerolínea en  
cada año?**

El Job está definido como un script de Spark de la siguiente manera:

Nombre: `job2_airline_delay`.
IAM Role: `LabRole`.
Versión de AWS Glue: `6.0`.
Lenguaje: Python.
Tipo de worker: `G.1X`.
Número de workers: `2`.

    import sys
    
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    
    from pyspark.context import SparkContext
    from pyspark.sql import functions as F
    
    # Inicializar Glue
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    # Rutas S3
    INPUT_PATH = "s3://almacen-bts-trabajo2/output/flights_enriched/"
    OUTPUT_PATH = "s3://almacen-bts-trabajo2/output/aggregations/airline_delay_by_year/"
    
    # Leer datos enriquecidos
    df = spark.read.parquet(INPUT_PATH)
    
    # Agrupar por año y aerolínea
    result = (
        df.groupBy("year", "reporting_airline")
        .agg(
            F.count("*").alias("total_vuelos"),
            F.sum(
                F.when(F.col("depdel15") == 1, 1).otherwise(0)
            ).alias("vuelos_retrasados"),
            F.sum(
                F.when(F.col("depdel15").isNull(), 1).otherwise(0)
            ).alias("vuelos_sin_clasificar")
        )
    )
    
    # Calcular porcentaje de retrasos entre vuelos con etiqueta conocida
    result = result.withColumn(
        "vuelos_clasificados",
        F.col("total_vuelos") - F.col("vuelos_sin_clasificar")
    )
    
    result = result.withColumn(
        "porcentaje_retrasos",
        F.when(
            F.col("vuelos_clasificados") > 0,
            F.round(
                F.col("vuelos_retrasados") / F.col("vuelos_clasificados") * 100,
                2
            )
        ).otherwise(None)
    )
    
    # Guardar resultado como Parquet
    (
        result.write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )
    
    print("Job 2 finalizado correctamente.")
    print(f"Resultado guardado en: {OUTPUT_PATH}")
    
    job.commit()
El Job lee los archivos Parquet de  
`s3://almacen-bts-trabajo2/output/flights_enriched/`. A partir de la  
columna `DepDel15`, identifica los vuelos retrasados (`DepDel15 = 1`) y  
los vuelos cuya clasificación no está disponible (`DepDel15` nulo).  
Después agrupa los datos por `year` y `reporting_airline`.

Con un Crawler podemos generar la tabla `airline_delay_by_year` para realizar la consulta en Athena.

    SELECT *
    FROM airline_delay_by_year
    ORDER BY year, reporting_airline
    LIMIT 20;

| Año | Aerolínea | Total vuelos | Vuelos retrasados | Sin clasificar | Clasificados | Retrasos (%) |
|---|---|---:|---:|---:|---:|---:|
| 2022 | 9E | 231941 | 32441 | 7989 | 223952 | 14.49 |
| 2022 | AA | 874145 | 184547 | 25175 | 848970 | 21.74 |
| 2022 | AS | 228384 | 39692 | 6038 | 222346 | 17.85 |
| 2022 | B6 | 273058 | 85558 | 9954 | 263104 | 32.52 |
| 2022 | DL | 893172 | 146112 | 14033 | 879139 | 16.62 |
| 2022 | F9 | 155482 | 48408 | 4334 | 151148 | 32.03 |
| 2022 | G4 | 116877 | 34381 | 4076 | 112801 | 30.48 |
| 2022 | HA | 73865 | 15611 | 669 | 73196 | 21.33 |
| 2022 | MQ | 248059 | 37596 | 5336 | 242723 | 15.49 |
| 2022 | NK | 232400 | 53667 | 6847 | 225553 | 23.79 |
| 2022 | OH | 214450 | 36197 | 6386 | 208064 | 17.40 |
| 2022 | OO | 733576 | 111771 | 13059 | 720517 | 15.51 |
| 2022 | QX | 88791 | 12975 | 1407 | 87384 | 14.85 |
| 2022 | UA | 626473 | 122482 | 11593 | 614880 | 19.92 |
| 2022 | WN | 1307149 | 364956 | 42353 | 1264796 | 28.85 |
| 2022 | YV | 114779 | 21809 | 3729 | 111050 | 19.64 |
| 2022 | YX | 316524 | 50123 | 14370 | 302154 | 16.59 |
| 2023 | 9E | 201520 | 26431 | 4049 | 197471 | 13.38 |
| 2023 | AA | 940531 | 205574 | 9514 | 931017 | 22.08 |
| 2023 | AS | 245344 | 43371 | 1790 | 243554 | 17.81 |
Los resultados obtenidos nos permiten responder la pregunta planteada, con informacion clara de los aeropuertos que mas restrasos obtuvieron en cada anualidad.
### 1.6.2 Job 3
El Job 3 esta configurado de la misma manera, a excepcion del nombre `job3_route_delay`y el codigo. Con la intencion de responder a la pregunta **¿Qué rutas aéreas (origen-destino) presentan el mayor porcentaje de vuelos retrasados?**

    import sys
    
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    
    from pyspark.context import SparkContext
    from pyspark.sql import functions as F
    
    # Inicializar Glue
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    # Rutas S3
    INPUT_PATH = "s3://almacen-bts-trabajo2/output/flights_enriched/"
    OUTPUT_PATH = "s3://almacen-bts-trabajo2/output/aggregations/route_delay/"
    
    # Leer datos enriquecidos
    df = spark.read.parquet(INPUT_PATH)
    
    # Agrupar por aeropuerto de origen y destino
    result = (
        df.groupBy("origin", "dest")
        .agg(
            F.count("*").alias("total_vuelos"),
            F.sum(
                F.when(F.col("depdel15") == 1, 1).otherwise(0)
            ).alias("vuelos_retrasados"),
            F.sum(
                F.when(F.col("depdel15").isNull(), 1).otherwise(0)
            ).alias("vuelos_sin_clasificar")
        )
    )
    
    # Calcular porcentaje de retrasos entre vuelos con etiqueta conocida
    result = result.withColumn(
        "vuelos_clasificados",
        F.col("total_vuelos") - F.col("vuelos_sin_clasificar")
    )
    
    result = result.withColumn(
        "porcentaje_retrasos",
        F.when(
            F.col("vuelos_clasificados") > 0,
            F.round(
                F.col("vuelos_retrasados") / F.col("vuelos_clasificados") * 100,
                2
            )
        ).otherwise(None)
    )
    
    # Guardar resultado como Parquet
    (
        result.write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )
    
    print("Job 3 finalizado correctamente.")
    print(f"Resultado guardado en: {OUTPUT_PATH}")
    
    job.commit()
    
De nuevo, creando un Crawler para los datos generados por el Job 3 podemos realizar la siguiente consulta.

    SELECT
        origin,
        dest,
        total_vuelos,
        vuelos_retrasados,
        vuelos_sin_clasificar,
        vuelos_clasificados,
        porcentaje_retrasos
    FROM route_delay
    WHERE vuelos_clasificados >= 1000
    ORDER BY porcentaje_retrasos DESC
    LIMIT 20;
Se filtran aquellos vuelos menores a 20 para tener en cuenta principalmente rutas concurridas. Obteniendo la siguiente informacion:
| # | Origen | Destino | Total vuelos | Vuelos retrasados | Sin clasificar | Clasificados | Retrasos (%) |
|---:|---|---|---:|---:|---:|---:|---:|
| 1 | DCA | AUS | 1703 | 861 | 29 | 1674 | 51.43 |
| 2 | BUR | JFK | 1494 | 651 | 60 | 1434 | 45.40 |
| 3 | JFK | SMF | 1265 | 556 | 15 | 1250 | 44.48 |
| 4 | MDW | SFO | 1400 | 612 | 16 | 1384 | 44.22 |
| 5 | ABQ | JFK | 1061 | 448 | 42 | 1019 | 43.96 |
| 6 | JFK | ABQ | 1061 | 453 | 30 | 1031 | 43.94 |
| 7 | DFW | MDT | 1704 | 727 | 44 | 1660 | 43.80 |
| 8 | DFW | SYR | 1610 | 686 | 38 | 1572 | 43.64 |
| 9 | CMH | SFO | 1388 | 598 | 12 | 1376 | 43.46 |
| 10 | DFW | GEG | 2606 | 1104 | 37 | 2569 | 42.97 |
| 11 | IAH | JFK | 1817 | 712 | 62 | 1755 | 40.57 |
| 12 | BDL | SJU | 4439 | 1763 | 82 | 4357 | 40.46 |
| 13 | DEN | BUF | 2205 | 872 | 34 | 2171 | 40.17 |
| 14 | DFW | BUF | 2358 | 917 | 57 | 2301 | 39.85 |
| 15 | DFW | BFL | 2307 | 884 | 51 | 2256 | 39.18 |
| 16 | MSY | SAN | 1150 | 447 | 9 | 1141 | 39.18 |
| 17 | DEN | OAK | 8403 | 3223 | 106 | 8297 | 38.85 |
| 18 | SNA | PVU | 1518 | 578 | 28 | 1490 | 38.79 |
| 19 | PGD | FNT | 1358 | 519 | 18 | 1340 | 38.73 |
| 20 | MDW | ROC | 1943 | 738 | 34 | 1909 | 38.66 |

Asi entonces, podemos determinar que rutas como DCA-AUS tienen una probabilidad superior al 50% de retrasarse. Informacion que podria fortalecer los modelos predictivos de retrasos con el enfoque adecuado.

## 1.7 Orquestación con AWS Step Functions

Para coordinar los tres trabajos se creó la máquina de estados:

`bts_flight_processing_workflow`

Se utilizó Amazon States Language (ASL) para definir el flujo de ejecución.

La máquina de estados utiliza la integración de AWS Glue `glue:startJobRun.sync`, que permite iniciar un trabajo y esperar a que termine antes de continuar.

El flujo se compone de dos etapas:

**Primera etapa: ejecución secuencial**

Se inicia `job1_flights_clean_join`. Step Functions espera a que finalice correctamente.

**Segunda etapa: ejecución paralela**

Cuando termina el primer trabajo, se activa un estado de tipo Parallel con dos ramas:

-   Rama 1: `job2_airline_delay`.
    
-   Rama 2: `job3_route_delay`.
    

Ambas ramas se ejecutan simultáneamente y la máquina de estados espera a que las dos finalicen.

La ejecución de prueba completó correctamente los tres trabajos y presentó el estado Succeeded. El tiempo total observado fue de aproximadamente 25 minutos.

Esta configuración evita ejecutar las agregaciones antes de que estén disponibles los datos enriquecidos.

El codigo correspondiente a la configuracion:

    {
      "Comment": "Pipeline BTS: limpieza, enriquecimiento y agregaciones paralelas",
      "QueryLanguage": "JSONPath",
      "StartAt": "Job1_Flights_Clean_Join",
      "States": {
        "Job1_Flights_Clean_Join": {
          "Type": "Task",
          "Resource": "arn:aws:states:::glue:startJobRun.sync",
          "Parameters": {
            "JobName": "job1_flights_clean_join"
          },
          "Next": "Parallel_Aggregations"
        },
        "Parallel_Aggregations": {
          "Type": "Parallel",
          "Branches": [
            {
              "StartAt": "Job2_Airline_Delay",
              "States": {
                "Job2_Airline_Delay": {
                  "Type": "Task",
                  "Resource": "arn:aws:states:::glue:startJobRun.sync",
                  "Parameters": {
                    "JobName": "job2_airline_delay"
                  },
                  "End": true
                }
              }
            },
            {
              "StartAt": "Job3_Route_Delay",
              "States": {
                "Job3_Route_Delay": {
                  "Type": "Task",
                  "Resource": "arn:aws:states:::glue:startJobRun.sync",
                  "Parameters": {
                    "JobName": "job3_route_delay"
                  },
                  "End": true
                }
              }
            }
          ],
          "End": true
        }
      }
    }

## 1.8 Automatización con Amazon EventBridge Scheduler
Se creó el Schedule:

`bts_daily_flight_processing`

Su destino es la máquina de estados:

`bts_flight_processing_workflow`

Se configuró una ejecución programada de una sola vez, utilizando la zona horaria `America/Bogota` y sin ventana flexible de ejecución.

De esta manera, EventBridge Scheduler inicia automáticamente la máquina de estados mediante la acción StartExecution.

La ejecución programada se realizó correctamente y permitió comprobar que el pipeline puede iniciarse sin necesidad de entrar manualmente a Step Functions y pulsar Start execution.

## 1.9 Resultados generales

La implementación permitió cumplir las etapas principales de la solución:

-   Almacenamiento de los archivos originales y procesados en Amazon S3.
    
-   Limpieza y enriquecimiento de los datos mediante AWS Glue y PySpark.
    
-   Generación de dos agregaciones analíticas independientes.
    
-   Escritura de resultados en formato Parquet.
    
-   Orquestación secuencial y paralela mediante AWS Step Functions.
    
-   Automatización del inicio del flujo mediante EventBridge Scheduler.
    
-   Registro de los resultados en Glue Data Catalog.
    
-   Consulta y validación de las agregaciones mediante Amazon Athena.
    

Los tres Glue Jobs se ejecutaron individualmente con éxito y posteriormente se ejecutaron correctamente desde Step Functions. También se comprobó la ejecución automática mediante EventBridge Scheduler.

## 1. 10. Conclusiones

El proyecto demuestra cómo diferentes servicios de AWS pueden integrarse para construir un pipeline de procesamiento de datos automatizado.

La separación del procesamiento en tres trabajos permite mantener responsabilidades independientes: primero se prepara y enriquece la información y posteriormente se realizan dos análisis distintos.

La utilización de Step Functions permite controlar las dependencias entre trabajos y ejecutar en paralelo aquellas operaciones que no dependen entre sí. Por su parte, EventBridge Scheduler permite automatizar el inicio del proceso.

Finalmente, el uso de Parquet facilita almacenar los resultados procesados y consultarlos mediante Athena, evitando tener que ejecutar nuevamente todo el procesamiento para responder preguntas analíticas.

En conclusión, se implementó una solución funcional que integra almacenamiento, procesamiento distribuido, orquestación, automatización y análisis de datos utilizando servicios administrados de AWS.

## 1.11 Clausula de Uso Etico de Inteligencia Artificial.
Se utilizo IA generativa para el desarrollo del codigo de los Jobs. Corrigiendola y ajustandola a las necesidades particulares del proyecto.

El formato Markdown para las tablas fue generado por IA usando los datos obtenidos por AWS, los datos **NO** fueron generados por IA.

El resumen de los resultados generales y las conclusiones fue generado por IA. Los resultados generales son recopilaciones de la arquitectura propuesta por el equipo.

El formato Json para la descripcion de la configuracion de AWS Step Functions fue generado por IA, sin embargo corresponde al planteamiento propuesto de un Job seguido de dos trabajos en paralelo.

Esta seccion **NO** fue generada por IA.
La produccion del video **NO** fue generada por IA.
