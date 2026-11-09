# SI7006-SI6003_Trabajo2

**Trabajo 2 de Almacenamiento y Procesamiento de Grandes Datos — 2026**

> Alejandro Quintero Moreno  
> Santiago Gomez Rueda
# 0. Introducción
La documentacion presentada tiene el proposito de explicar el procedimiento paso paso sobre para implementar un pipeline batch con Apache Spark en AWS y un flujo de Streaming con Amazon Kinesis Data Streams.

Video:

https://www.youtube.com/watch?v=G7aoWtoy91A

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

## 2. Parte B

### 2.1 Objetivo y contexto

La Parte B construye un flujo de streaming con Amazon Kinesis Data Streams. Mientras que la Parte A procesa en lote datos históricos, esta parte procesa eventos de forma continua: un productor publica eventos durante varios minutos y una aplicación calcula, por ventanas de un minuto, el conteo de eventos y el promedio de un campo, por categoría.

El flujo debe manejar dos situaciones propias de los datos reales en streaming:

- **Eventos repetidos:** el mismo evento llega más de una vez y no debe contarse dos veces.
- **Eventos inválidos:** eventos con un campo faltante o con un valor fuera de rango, que deben apartarse junto con el motivo del descarte.

La pregunta que responde esta parte es:

> ¿Qué categoría tuvo el mayor promedio de valor y en qué minuto hubo más eventos únicos?

> **Nota:** los eventos que publica el productor son **sintéticos**. Se generan aleatoriamente para demostrar el funcionamiento del pipeline, por lo que los resultados no describen una situación real.

### 2.2 Arquitectura

```
                         ┌─► Lambda procesador ─┬─► DynamoDB: resultados_ventanas
Productor ──► Kinesis ───┤   (valida, deduplica,├─► DynamoDB: eventos_invalidos
(Python)      Data       │    agrega por minuto)├─► DynamoDB: eventos_vistos
              Stream     │                      └─► DynamoDB: contadores
                         │
                         └─► Lambda raw ──────────► S3 (eventos tal como llegan)
```

| Componente | Función |
|---|---|
| **Kinesis Data Streams** | Buzón ordenado y duradero donde el productor publica y de donde leen los consumidores. Un *shard* ofrece 1 MB/s de escritura y 2 MB/s de lectura. |
| **Lambda `procesador`** | Primer consumidor. Valida cada evento, detecta repetidos y calcula las ventanas. |
| **Lambda `raw`** | Segundo consumidor del mismo stream. Guarda todos los eventos sin filtrar. |
| **DynamoDB** | Almacena los resultados por ventana, los identificadores ya vistos, los eventos inválidos con su motivo y los contadores de control. |
| **S3** | Almacena los eventos crudos en formato JSON Lines. |

Cada *trigger* de Kinesis en una Lambda es un consumidor independiente. Por eso `procesador` y `raw` leen el stream completo de forma separada.

Para la aplicación de ventanas se eligió **AWS Lambda** en lugar de Amazon Managed Service for Apache Flink. El enunciado permite ambas opciones, y Lambda resulta más económica y simple de depurar dentro del presupuesto del Learner Lab.

### 2.3 Diseño del flujo

**Esquema del evento**

```json
{
  "event_id": "uuid",
  "categoria": "A",
  "valor": 123.45,
  "timestamp": "2026-11-09T15:04:12.345Z"
}
```

| Campo | Significado |
|---|---|
| `event_id` | Identificador único del evento. Se usa para reconocer repetidos. |
| `categoria` | Categoría del evento (A, B, C o D). Es la clave de la agregación. |
| `valor` | Campo numérico del que se calcula el promedio. |
| `timestamp` | Momento del evento, en UTC. |

**Reglas de validez.** Un evento es válido si cumple todo lo siguiente:

- Contiene los cuatro campos.
- La categoría es A, B, C o D.
- El valor es numérico y está entre 0 y 1000.

Los eventos que no cumplen se apartan con el motivo del descarte: `campo faltante`, `categoria desconocida`, `valor fuera de rango`, `valor no numerico` o `JSON malformado`.

**Decisiones de diseño**

| Requisito | Solución |
|---|---|
| Reconocer repetidos | Un evento es repetido si su `event_id` ya existe en la tabla `eventos_vistos`. |
| Ventana de un minuto | Se trunca el `timestamp` del evento al minuto (`2026-11-09T15:04`). Corresponde a una ventana *tumbling* sin solapamiento, basada en el tiempo del evento. |
| Conteo y promedio | Por cada evento único se suma 1 al conteo y el valor a la suma. El promedio es `suma / conteo`. |
| No contar dos veces | Una transacción de DynamoDB inserta el `event_id` solo si no existía, actualiza la ventana y actualiza el contador. Si el identificador ya existía, la transacción se cancela y nada se cuenta. |

### 2.4 Preparación del entorno

1. Iniciar el Learner Lab con **Start Lab** y esperar el indicador verde.
2. Abrir la consola de AWS y cambiar la región a **N. Virginia (us-east-1)**. Todos los recursos deben estar en la misma región.
3. Para mantener la consistencia de roles se usa **LabRole** en todas las funciones, ya que AWS Academy no permite crear roles.

### 2.5 Creación del stream de Kinesis

1. En el buscador de AWS escribimos **Kinesis** y entramos a **Kinesis Data Streams**.
2. Pulsamos **Create data stream** con la siguiente configuración:
   - Nombre: `eventos-stream`
   - Capacity mode: **Provisioned**
   - Provisioned shards: **1**
3. Pulsamos **Create data stream** y esperamos el estado **Active**.

Un shard es suficiente para unos 5 eventos por segundo. El modo provisionado con un solo shard es la opción más económica para este volumen.

### 2.6 Creación de las tablas de DynamoDB

En **DynamoDB → Create table** creamos cuatro tablas con claves de tipo String y capacidad **On-demand**:

| Tabla | Partition key | Sort key | Uso |
|---|---|---|---|
| `resultados_ventanas` | `categoria` | `ventana` | Una fila por categoría y minuto |
| `eventos_vistos` | `event_id` | (ninguna) | Memoria de identificadores para detectar repetidos |
| `eventos_invalidos` | `id` | (ninguna) | Eventos descartados con su motivo |
| `contadores` | `id` | (ninguna) | Contadores de únicos, repetidos e inválidos |

En `eventos_vistos` activamos el **TTL**: en la pestaña **Additional settings** pulsamos **Turn on** en *Time to Live* y escribimos el atributo `ttl`. Así los identificadores antiguos se eliminan solos y la tabla no crece sin límite.

La tabla `eventos_invalidos` usa como clave el `sequenceNumber` de Kinesis, que es único por registro. De esta forma un reintento de Lambda no duplica el mismo evento inválido.

### 2.7 Creación del bucket de S3

Creamos un bucket para los eventos crudos con **Block all public access** activado, en la misma región. Los eventos se guardarán bajo el prefijo `raw/`.

```
eventos-crudo/
└── raw/
    └── AAAA/MM/DD/HH/<sequenceNumber>.jsonl
```

### 2.8 Lambda `procesador`

En **Lambda → Create function** definimos:

- Function name: `procesador`
- Runtime: Python 3.12
- Execution role: **LabRole** (rol existente)
- Timeout: 1 minuto (en *Configuration → General configuration*)

El handler queda con el valor por defecto, `lambda_function.lambda_handler`. En el editor de código pegamos lo siguiente y pulsamos **Deploy**:

```python
import base64, json, time
from decimal import Decimal
import boto3

ddb = boto3.client("dynamodb")
T_RES, T_VISTOS = "resultados_ventanas", "eventos_vistos"
T_INV, T_CONT = "eventos_invalidos", "contadores"

CAMPOS = ["event_id", "categoria", "valor", "timestamp"]
CATEGORIAS = {"A", "B", "C", "D"}
MIN_V, MAX_V = 0, 1000


def validar(e):
    """Devuelve el motivo de descarte, o None si el evento es válido."""
    if not isinstance(e, dict):
        return "no es un objeto JSON"
    for c in CAMPOS:
        if e.get(c) in (None, ""):
            return f"campo faltante: {c}"
    if e["categoria"] not in CATEGORIAS:
        return f"categoria desconocida: {e['categoria']}"
    try:
        v = float(e["valor"])
    except (TypeError, ValueError):
        return "valor no numerico"
    if not (MIN_V <= v <= MAX_V):
        return f"valor fuera de rango [{MIN_V},{MAX_V}]: {v}"
    return None


def contador(campo):
    return {"Update": {
        "TableName": T_CONT, "Key": {"id": {"S": "total"}},
        "UpdateExpression": f"ADD {campo} :uno",
        "ExpressionAttributeValues": {":uno": {"N": "1"}}}}


def lambda_handler(event, context):
    for rec in event["Records"]:
        seq = rec["kinesis"]["sequenceNumber"]
        crudo = base64.b64decode(rec["kinesis"]["data"]).decode("utf-8", "replace")

        # 1) Parsear y validar
        try:
            e = json.loads(crudo)
            motivo = validar(e)
        except json.JSONDecodeError:
            e, motivo = None, "JSON malformado"

        # 2) Inválido: guardarlo con su motivo y sumar al contador (atómico)
        if motivo:
            try:
                ddb.transact_write_items(TransactItems=[
                    {"Put": {"TableName": T_INV,
                             "Item": {"id": {"S": seq}, "motivo": {"S": motivo},
                                      "payload": {"S": crudo}},
                             "ConditionExpression": "attribute_not_exists(id)"}},
                    contador("invalidos")])
            except ddb.exceptions.TransactionCanceledException:
                pass  # ya se había guardado (reintento): no contar de nuevo
            continue

        # 3) Válido: deduplicación, ventana y contador en UNA transacción
        ventana = e["timestamp"][:16]            # '2026-11-09T15:04' = minuto
        valor = str(Decimal(str(e["valor"])))
        ttl = int(time.time()) + 24 * 3600
        try:
            ddb.transact_write_items(TransactItems=[
                {"Put": {"TableName": T_VISTOS,
                         "Item": {"event_id": {"S": e["event_id"]}, "ttl": {"N": str(ttl)}},
                         "ConditionExpression": "attribute_not_exists(event_id)"}},
                {"Update": {"TableName": T_RES,
                            "Key": {"categoria": {"S": e["categoria"]}, "ventana": {"S": ventana}},
                            "UpdateExpression": "ADD conteo :uno, suma :v",
                            "ExpressionAttributeValues": {":uno": {"N": "1"}, ":v": {"N": valor}}}},
                contador("unicos")])
        except ddb.exceptions.TransactionCanceledException as ex:
            razones = [r.get("Code") for r in ex.response.get("CancellationReasons", [])]
            if "ConditionalCheckFailed" in razones:   # el event_id ya existía = repetido
                ddb.update_item(TableName=T_CONT, Key={"id": {"S": "total"}},
                                UpdateExpression="ADD repetidos :uno",
                                ExpressionAttributeValues={":uno": {"N": "1"}})
            else:
                raise   # error real: Lambda reintentará el lote
```

**Cómo funciona:**

- Kinesis entrega los datos codificados en base64, por lo que primero se decodifican.
- Si el evento es inválido, se guarda con su `motivo` y se incrementa el contador `invalidos`.
- Si es válido, una transacción de DynamoDB ejecuta tres operaciones de forma atómica: registrar el `event_id`, actualizar la ventana y sumar al contador `unicos`.
- Si el `event_id` ya existía, la condición falla, la transacción se cancela y el evento no entra a la ventana. Solo se incrementa `repetidos`.
- La instrucción `ADD` de DynamoDB es un incremento atómico, por lo que no hay problemas con eventos concurrentes.

### 2.9 Lambda `raw`

Se crea una segunda función con la misma configuración: nombre `raw`, Python 3.12, **LabRole** y timeout de 1 minuto. Se reemplaza el valor de `BUCKET` por el nombre del bucket:

```python
import base64, datetime as dt, boto3

s3 = boto3.client("s3")
BUCKET = "eventos-crudo"

def lambda_handler(event, context):
    recs = event["Records"]
    lineas = [base64.b64decode(r["kinesis"]["data"]).decode("utf-8", "replace") for r in recs]
    primero = recs[0]["kinesis"]
    llegada = dt.datetime.fromtimestamp(primero["approximateArrivalTimestamp"], dt.timezone.utc)
    key = f"raw/{llegada:%Y/%m/%d/%H}/{primero['sequenceNumber']}.jsonl"
    s3.put_object(Bucket=BUCKET, Key=key, Body="\n".join(lineas).encode("utf-8"))
```

**Cómo funciona:** cada lote se escribe tal como llegó, sin validar ni filtrar, en formato JSON Lines (un evento por línea). El nombre del archivo usa el `sequenceNumber` del primer registro, que es determinista. Si Lambda reintenta un lote, sobrescribe el mismo objeto en lugar de crear un duplicado.

### 2.10 Conexión de las Lambdas con el stream

En cada una de las dos funciones se agrega un trigger con **Add trigger → Kinesis**:

| Parámetro | Valor |
|---|---|
| Kinesis stream | `eventos-stream` |
| Consumer | (vacío) |
| Batch size | 100 |
| Batch window | 5 segundos |
| Starting position | **Trim horizon** |

Cada trigger es un *event source mapping*, un proceso gestionado que lee el stream y llama a la función con lotes de registros. Como cada Lambda tiene el suyo, ambas reciben todos los eventos de manera independiente, lo que cumple el requisito del segundo consumidor.

- **Batch size y window:** Lambda junta hasta 100 eventos o espera 5 segundos antes de invocar la función.
- **Trim horizon:** la lectura comienza en el evento más antiguo disponible, para no perder ninguno de los ya publicados.
- **Sin consumidor dedicado:** ambos triggers comparten el ancho de banda de lectura del shard, lo cual es suficiente para este volumen.

### 2.11 El productor

Los scripts se ejecutan desde **AWS CloudShell**, que usa las credenciales del Learner Lab y ya incluye Python y boto3. El productor publica durante **6 minutos** (el mínimo exigido es 5) a unos 5 eventos por segundo.

```python
import boto3, json, random, time, uuid, datetime as dt

STREAM = "eventos-stream"
DURACION_S = 6 * 60          # 6 minutos (>5 exigidos)
EVENTOS_POR_SEG = 5
kin = boto3.client("kinesis", region_name="us-east-1")

res = {"publicados": 0, "validos_unicos": 0, "duplicados": 0, "invalidos": 0}
recientes = []   # válidos ya enviados, para repetirlos

def evento_valido():
    return {"event_id": str(uuid.uuid4()),
            "categoria": random.choice("ABCD"),
            "valor": round(random.uniform(1, 500), 2),
            "timestamp": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"}

def evento_invalido():
    e = evento_valido()
    tipo = random.choice(["sin_campo", "fuera_rango", "categoria_mala"])
    if tipo == "sin_campo":
        del e[random.choice(["event_id", "categoria", "valor", "timestamp"])]
    elif tipo == "fuera_rango":
        e["valor"] = random.choice([-50, 5000])
    else:
        e["categoria"] = "Z"
    return e

def enviar(lote):
    """lote: lista de (tipo, evento). Reintenta los registros que Kinesis rechace."""
    pendientes = lote
    while pendientes:
        r = kin.put_records(StreamName=STREAM, Records=[
            {"Data": json.dumps(ev).encode(), "PartitionKey": str(uuid.uuid4())}
            for _, ev in pendientes])
        fallidos = []
        for item, resp in zip(pendientes, r["Records"]):
            if "ErrorCode" in resp:
                fallidos.append(item)
            else:
                res["publicados"] += 1
                res[item[0]] += 1
        pendientes = fallidos
        if pendientes:
            time.sleep(0.5)

inicio = time.time()
while time.time() - inicio < DURACION_S:
    lote = []
    for _ in range(EVENTOS_POR_SEG):
        x = random.random()
        if x < 0.10 and recientes:                       # ~10% repetidos
            lote.append(("duplicados", random.choice(recientes)))
        elif x < 0.20:                                   # ~10% inválidos
            lote.append(("invalidos", evento_invalido()))
        else:                                            # ~80% válidos únicos
            e = evento_valido()
            recientes = (recientes + [e])[-200:]
            lote.append(("validos_unicos", e))
    enviar(lote)
    print(res, end="\r")
    time.sleep(1)

json.dump(res, open("resumen_productor.json", "w"), indent=2)
print("\nListo:", res)
```

**Puntos clave:**

- Los **repetidos** son una copia exacta de un evento válido enviado antes, con el mismo `event_id`. Representan cerca del 10 % de los eventos.
- Los **inválidos** representan cerca del 10 % y pueden tener un campo faltante, una categoría inexistente (`Z`) o un valor fuera del rango definido (-50 o 5000).
- `PutRecords` puede aceptar unos registros y rechazar otros. Por eso el productor revisa cada respuesta, reintenta los rechazados y solo cuenta como publicado lo que Kinesis aceptó.
- El productor lleva su propia contabilidad en `resumen_productor.json`, que es la referencia contra la cual se verifican los conteos al final.
- La `PartitionKey` aleatoria reparte los registros entre shards. Con un solo shard no tiene efecto práctico.

### 2.12 Consulta de resultados

El siguiente script lee la tabla `resultados_ventanas` y responde la pregunta del proyecto:

```python
import boto3
from collections import defaultdict

t = boto3.resource("dynamodb", region_name="us-east-1").Table("resultados_ventanas")
items, resp = [], t.scan()
items += resp["Items"]
while "LastEvaluatedKey" in resp:
    resp = t.scan(ExclusiveStartKey=resp["LastEvaluatedKey"]); items += resp["Items"]

print(f"{'categoria':<10}{'ventana':<18}{'conteo':>7}{'promedio':>10}")
for i in sorted(items, key=lambda x: (x["ventana"], x["categoria"])):
    print(f"{i['categoria']:<10}{i['ventana']:<18}{int(i['conteo']):>7}{float(i['suma'])/int(i['conteo']):>10.2f}")

tot = defaultdict(lambda: [0, 0.0]); por_min = defaultdict(int)
for i in items:
    tot[i["categoria"]][0] += int(i["conteo"]); tot[i["categoria"]][1] += float(i["suma"])
    por_min[i["ventana"]] += int(i["conteo"])
mejor = max(tot, key=lambda c: tot[c][1] / tot[c][0])
print(f"\nCategoría con mayor promedio: {mejor} ({tot[mejor][1]/tot[mejor][0]:.2f})")
m = max(por_min, key=por_min.get)
print(f"Minuto con más eventos únicos: {m} ({por_min[m]} eventos)")
```

La salida muestra el detalle por ventana, y al final las dos respuestas: la categoría con el mayor promedio de valor y el minuto con más eventos únicos.

### 2.13 Verificación de los conteos

El siguiente script demuestra que **publicados = únicos + repetidos + inválidos**, y que ese total coincide con los eventos guardados en S3:

```python
import boto3, json

R = "us-east-1"
ddb, s3 = boto3.client("dynamodb", region_name=R), boto3.client("s3", region_name=R)
BUCKET = "eventos-crudo"

prod = json.load(open("resumen_productor.json"))
c = ddb.get_item(TableName="contadores", Key={"id": {"S": "total"}})["Item"]
g = lambda k: int(c.get(k, {"N": "0"})["N"])
unicos, repetidos, invalidos = g("unicos"), g("repetidos"), g("invalidos")

# Líneas guardadas en S3 (eventos crudos)
raw = 0
for pag in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="raw/"):
    for o in pag.get("Contents", []):
        cuerpo = s3.get_object(Bucket=BUCKET, Key=o["Key"])["Body"].read().decode()
        raw += len([l for l in cuerpo.split("\n") if l.strip()])

# Filas en la tabla de inválidos y suma de conteos de las ventanas
n_inv = ddb.scan(TableName="eventos_invalidos", Select="COUNT")["Count"]
suma_ventanas, kw = 0, {}
while True:
    r = ddb.scan(TableName="resultados_ventanas", **kw)
    suma_ventanas += sum(int(i["conteo"]["N"]) for i in r["Items"])
    if "LastEvaluatedKey" not in r: break
    kw = {"ExclusiveStartKey": r["LastEvaluatedKey"]}

print(f"Publicados (productor):          {prod['publicados']}")
print(f"Guardados crudos en S3:          {raw}")
print(f"Únicos procesados:               {unicos}")
print(f"Repetidos descartados:           {repetidos}")
print(f"Inválidos descartados:           {invalidos}")
print(f"Suma únicos+repetidos+inválidos: {unicos+repetidos+invalidos}")
print(f"Suma de 'conteo' en ventanas:    {suma_ventanas}   | filas en eventos_invalidos: {n_inv}")
print("\nCUADRA ✅" if prod["publicados"] == raw == unicos+repetidos+invalidos and
      unicos == prod["validos_unicos"] == suma_ventanas and
      repetidos == prod["duplicados"] and invalidos == prod["invalidos"] == n_inv
      else "\nNO CUADRA ❌ (espera unos segundos y reintenta, o revisa logs)")
```

La verificación cruza cuatro fuentes independientes:

1. **El productor:** cuántos eventos aceptó Kinesis y de qué tipo eran.
2. **DynamoDB, tabla `contadores`:** cuántos eventos únicos, repetidos e inválidos procesó la Lambda.
3. **DynamoDB, tablas de resultados e inválidos:** que la suma de los conteos de las ventanas y el número de filas inválidas coincidan con los contadores.
4. **S3:** que el total de eventos crudos sea igual al total publicado.

### 2.14 Ejecución

1. Dejar los datos en cero con `reset.py` (ver sección 2.16).
2. Verificar que los dos triggers estén en **Enabled**.
3. Ejecutar el productor desde CloudShell con `python3 productor.py`. Dura 6 minutos y la pestaña no debe cerrarse.
4. Esperar unos 60 segundos a que los consumidores alcancen el final del stream.
5. Ejecutar `python3 consulta.py` y luego `python3 verificar.py`.
6. Revisar las tablas y el bucket en la consola para documentar los resultados.

### 2.15 Resultados

> Los valores de las tablas deben completarse con la salida de la ejecución final.

#### 2.15.1 Resultados por ventana (DynamoDB)

La tabla `resultados_ventanas` contiene una fila por categoría y minuto, con los campos `conteo` y `suma`. El promedio se obtiene como `suma / conteo`.

| Categoría | Ventana | Conteo | Promedio |
|---|---|---:|---:|
| A | 2026-11-09T21:18 | 68 | 252.69 |
| B | 2026-11-09T21:18 | 60 | 236.55 |
| C | 2026-11-09T21:18 | 50 | 212.12 |
| D | 2026-11-09T21:18 | 53 | 258.71 |
| A | 2026-11-09T21:19 | 62 | 293.86 |
| B | 2026-11-09T21:19 | 55 | 254.82 |
| C | 2026-11-09T21:19 | 64 | 233.71 |
| D | 2026-11-09T21:19 | 55 | 248.18 |
| A | 2026-11-09T21:20 | 60 | 254.35 |
| B | 2026-11-09T21:20 | 59 | 268.76 |
| C | 2026-11-09T21:20 | 57 | 238.75 |
| D | 2026-11-09T21:20 | 63 | 238.46 |
| A | 2026-11-09T21:21 | 66 | 249.82 |
| B | 2026-11-09T21:21 | 49 | 230.47 |
| C | 2026-11-09T21:21 | 53 | 246.00 |
| D | 2026-11-09T21:21 | 70 | 245.60 |
| A | 2026-11-09T21:22 | 63 | 233.39 |
| B | 2026-11-09T21:22 | 57 | 262.14 |
| C | 2026-11-09T21:22 | 65 | 259.56 |
| D | 2026-11-09T21:22 | 44 | 242.01 |
| A | 2026-11-09T21:23 | 68 | 250.29 |
| B | 2026-11-09T21:23 | 56 | 236.08 |
| C | 2026-11-09T21:23 | 41 | 241.92 |
| D | 2026-11-09T21:23 | 52 | 272.00 |
| A | 2026-11-09T21:24 | 2 | 250.15 |
| B | 2026-11-09T21:24 | 1 | 239.00 |
| D | 2026-11-09T21:24 | 1 | 492.81 |

#### 2.15.2 Respuesta a la pregunta del proyecto

Categoría con mayor promedio: A (255.46)
Minuto con más eventos únicos: 2026-11-09T21:20 (239 eventos)

#### 2.15.3 Cuadre de conteos

| Concepto | Cantidad |
|---|---|
| Publicados (productor) | 1770 |
| Guardados crudos en S3 | 1770 |
| Únicos procesados | 1394 |
| Repetidos descartados | 196 |
| Inválidos descartados | 180 |
| **Únicos + repetidos + inválidos** | 1770 |

Resultado de `verificar.py`: CUADRA ✅.

***Cada ejecucion del flujo genera outputs distintos***

### 2.16 Pausa y repetición de la ejecución

El presupuesto del Learner Lab es limitado, por lo que al terminar cada sesión se pausan los componentes que generan costo.

| Recurso | Acción | Costo en reposo |
|---|---|---|
| Triggers de Lambda | **Disable** en cada trigger, sin borrarlos | Ninguno |
| Funciones Lambda | Se dejan creadas | Ninguno, solo cobran al ejecutarse |
| DynamoDB (on-demand) | Se dejan creadas | Prácticamente ninguno |
| S3 | Se deja el bucket | Prácticamente ninguno |
| Kinesis | No puede pausarse: el shard cobra por hora | Bajo, pero continuo |

Para repetir la demostración se ejecuta `reset.py`, que vacía las cuatro tablas y el prefijo `raw/`. Esto es necesario porque `verificar.py` compara contra el `resumen_productor.json` de la última ejecución. Si los datos anteriores siguen en las tablas, los conteos no cuadrarían.

```python
import boto3

R = "us-east-1"
BUCKET = "eventos-crudo"
CLAVES = {"resultados_ventanas": ["categoria", "ventana"], "eventos_vistos": ["event_id"],
          "eventos_invalidos": ["id"], "contadores": ["id"]}

dd = boto3.resource("dynamodb", region_name=R)
for nombre, claves in CLAVES.items():
    tabla, n, kw = dd.Table(nombre), 0, {}
    while True:
        r = tabla.scan(ProjectionExpression=", ".join("#" + k for k in claves),
                       ExpressionAttributeNames={"#" + k: k for k in claves}, **kw)
        with tabla.batch_writer() as bw:
            for it in r["Items"]:
                bw.delete_item(Key=it); n += 1
        if "LastEvaluatedKey" not in r: break
        kw = {"ExclusiveStartKey": r["LastEvaluatedKey"]}
    print(f"{nombre}: {n} filas borradas")

boto3.resource("s3", region_name=R).Bucket(BUCKET).objects.filter(Prefix="raw/").delete()
print("S3 raw/ vaciado")
```

Pasos para repetir la ejecución:

1. Iniciar el Learner Lab y confirmar la región us-east-1.
2. Ejecutar `python3 reset.py`.
3. Activar los triggers (**Enable**) y esperar el estado **Enabled**.
4. Ejecutar el productor, esperar 60 segundos y correr `consulta.py` y `verificar.py`.

### 2.17 Limitaciones

- **Reintentos de Lambda:** si una función falla a mitad de un lote y Kinesis lo reintenta, los eventos que ya se habían procesado se detectan como repetidos y pueden inflar ese contador. Con reintentos poco frecuentes el efecto es nulo, pero es una limitación del diseño.
- **Datos sintéticos:** los eventos se generan aleatoriamente, por lo que los resultados solo demuestran el funcionamiento del pipeline.
- **Ventanas por tiempo del evento:** el diseño asigna cada evento a la ventana de su `timestamp`. No cierra ventanas ni maneja eventos tardíos como lo haría un motor con *watermarks*, que es una de las ventajas de Apache Flink.
- **Costo del shard:** Kinesis no permite pausar el stream, por lo que se mantiene o se elimina y se recrea según el tiempo entre sesiones.

### 2.18 Conclusiones

El flujo implementado integra Kinesis, Lambda, DynamoDB y S3 para procesar eventos de forma continua. Cada evento se valida, se deduplica y se agrega por categoría y minuto, sin perder ni contar dos veces ningún registro.

El uso de dos consumidores independientes sobre el mismo stream separa responsabilidades: uno calcula y publica resultados, y el otro conserva la copia íntegra de los eventos para auditoría y reprocesamiento.

La transacción de DynamoDB resuelve la deduplicación de forma sencilla, ya que registrar el identificador y actualizar la ventana ocurren juntos o no ocurren. La verificación final demuestra que el número de eventos publicados es igual a la suma de los únicos procesados, los repetidos descartados y los inválidos descartados.

Finalmente, la elección de Lambda frente a Flink muestra un compromiso habitual: Lambda es más simple y económica para volúmenes pequeños, mientras que Flink ofrece ventanas con *watermarks* y estado administrado, útiles cuando el volumen y la complejidad de la lógica temporal crecen.

## 2.19 Clausula de Uso Etico de Inteligencia Artificial.
Se utilizo IA generativa para el desarrollo del los codigos de python y los codigos de las lambdas. 

El resumen de los resultados generales y las conclusiones fue generado por IA. Los resultados generales son recopilaciones de la arquitectura propuesta por el equipo.

Esta seccion de la documentacion PARTE B fue generada con asistencia de IA.
La produccion del video **NO** fue generada por IA.
