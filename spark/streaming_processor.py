"""
streaming_processor.py — Pipeline de Spark Structured Streaming.

Consume el tópico server_metrics de Kafka y aplica tres transformaciones
con DataFrames:

    1. FILTRADO     descarta registros nulos o inválidos
    2. PROYECCIÓN   renombra columnas y crea nuevas (event_time, nivel)
    3. AGREGACIÓN   promedio, máximo y conteo por servidor y métrica en
                    ventanas de 1 minuto (con watermark)

Salidas (sinks) en consola:
    - alertas_criticas: cada lectura > 80 % apenas llega (modo append)
    - metricas_por_ventana: agregados por ventana que se actualizan en cada
      micro-batch (modo update)

Ejecución (desde docker compose):
    docker compose up spark

Fuente alternativa sin Kafka (carpeta de archivos JSON):
    spark-submit spark/streaming_processor.py --source files --input-dir spark/input
"""

import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

TOPIC = "server_metrics"
VALID_METRICS = ["cpu_usage", "memory_usage"]
CRITICAL_THRESHOLD = 80.0
HIGH_THRESHOLD = 60.0

# Esquema ESTRICTO: en streaming Spark no puede inferirlo, porque los datos
# todavía no llegaron. Todo campo que no matchee el tipo queda en null.
METRIC_SCHEMA = StructType([
    StructField("timestamp", StringType(), nullable=True),
    StructField("server_id", StringType(), nullable=True),
    StructField("metric_name", StringType(), nullable=True),
    StructField("value", DoubleType(), nullable=True),
])


# ---------------------------------------------------------------------------
# Fuentes
# ---------------------------------------------------------------------------
def read_from_kafka(spark: SparkSession, bootstrap_servers: str) -> DataFrame:
    """Lee el tópico y parsea el JSON del value con el esquema estricto.
    Conserva la partición y el offset de Kafka para trazabilidad."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    return raw.select(
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.from_json(F.col("value").cast("string"), METRIC_SCHEMA).alias("data"),
    ).select("kafka_partition", "kafka_offset", "data.*")


def read_from_files(spark: SparkSession, input_dir: str) -> DataFrame:
    """Fuente alternativa: cada archivo JSON nuevo en la carpeta es un evento."""
    return (
        spark.readStream.schema(METRIC_SCHEMA)
        .option("maxFilesPerTrigger", 50)
        .json(input_dir)
        .withColumn("kafka_partition", F.lit(None).cast("int"))
        .withColumn("kafka_offset", F.lit(None).cast("long"))
    )


# ---------------------------------------------------------------------------
# Transformaciones (no ejecutan nada: solo construyen el plan)
# ---------------------------------------------------------------------------
def filter_valid(df: DataFrame) -> DataFrame:
    """1. FILTRADO: descarta nulos, métricas desconocidas y valores fuera de 0-100."""
    return df.filter(
        F.col("timestamp").isNotNull()
        & F.col("server_id").isNotNull()
        & F.col("metric_name").isin(VALID_METRICS)
        & F.col("value").isNotNull()
        & F.col("value").between(0, 100)
    )


def project(df: DataFrame) -> DataFrame:
    """2. PROYECCIÓN: event_time real, columna renombrada y nivel calculado."""
    return (
        df.withColumn("event_time", F.to_timestamp("timestamp"))
        .withColumnRenamed("value", "usage_pct")
        .withColumn(
            "nivel",
            F.when(F.col("usage_pct") > CRITICAL_THRESHOLD, "CRITICO")
            .when(F.col("usage_pct") > HIGH_THRESHOLD, "ALTO")
            .otherwise("NORMAL"),
        )
        .filter(F.col("event_time").isNotNull())  # timestamp con formato inválido
        .select("event_time", "server_id", "metric_name", "usage_pct", "nivel",
                "kafka_partition", "kafka_offset")
    )


def aggregate_by_window(df: DataFrame, window: str = "1 minute", watermark: str = "30 seconds") -> DataFrame:
    """3. AGREGACIÓN: ventanas tumbling por servidor y métrica.
    El watermark define cuánto se esperan eventos atrasados antes de cerrar
    una ventana (y permite a Spark liberar el estado de ventanas viejas)."""
    return (
        df.withWatermark("event_time", watermark)
        .groupBy(F.window("event_time", window), "server_id", "metric_name")
        .agg(
            F.round(F.avg("usage_pct"), 1).alias("promedio_pct"),
            F.max("usage_pct").alias("maximo_pct"),
            F.count("*").alias("lecturas"),
            F.sum(F.when(F.col("nivel") == "CRITICO", 1).otherwise(0)).alias("lecturas_criticas"),
        )
        .select(
            F.date_format("window.start", "HH:mm:ss").alias("ventana_desde"),
            F.date_format("window.end", "HH:mm:ss").alias("ventana_hasta"),
            "server_id", "metric_name", "promedio_pct", "maximo_pct",
            "lecturas", "lecturas_criticas",
        )
    )


def critical_alerts(df: DataFrame) -> DataFrame:
    """Lecturas críticas individuales, para reaccionar sin esperar a que cierre la ventana."""
    return df.filter(F.col("nivel") == "CRITICO").select(
        F.date_format("event_time", "HH:mm:ss").alias("hora"),
        "server_id", "metric_name", "usage_pct", "kafka_partition", "kafka_offset",
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Spark Structured Streaming sobre server_metrics")
    parser.add_argument("--source", choices=["kafka", "files"], default="kafka")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--input-dir", default="spark/input")
    parser.add_argument("--trigger", default="10 seconds", help="intervalo de micro-batch")
    parser.add_argument("--checkpoint-dir", default="/tmp/spark-checkpoints")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("server-metrics-streaming")
        # Agregaciones con 3 particiones de shuffle (default 200: demasiado para este volumen)
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    source = (read_from_kafka(spark, args.bootstrap_servers) if args.source == "kafka"
              else read_from_files(spark, args.input_dir))

    enriched = project(filter_valid(source))

    # Acciones: .start() es lo que dispara la ejecución del plan.
    alerts_query = (
        critical_alerts(enriched).writeStream
        .queryName("alertas_criticas")
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", f"{args.checkpoint_dir}/alertas")
        .trigger(processingTime=args.trigger)
        .start()
    )

    windows_query = (
        aggregate_by_window(enriched).writeStream
        .queryName("metricas_por_ventana")
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 50)
        .option("checkpointLocation", f"{args.checkpoint_dir}/ventanas")
        .trigger(processingTime=args.trigger)
        .start()
    )

    print(f"Streaming iniciado | fuente: {args.source} | trigger: {args.trigger} | "
          f"Spark UI: http://localhost:4040 | queries: {alerts_query.name}, {windows_query.name}")

    # Sin esto el script termina enseguida y mata las queries.
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
