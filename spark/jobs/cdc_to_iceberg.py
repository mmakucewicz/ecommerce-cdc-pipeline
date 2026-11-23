"""
Streams Debezium CDC events from Kafka into Apache Iceberg tables.

Medallion layout:
  bronze.cdc_events   every change event, append-only full history (idempotent on Kafka coordinates)
  silver.<table>      current state of each source table (MERGE: insert / update / delete)

Each micro-batch is processed in foreachBatch, which lets us run Iceberg MERGE INTO
statements on the batch - something a plain streaming sink cannot do.
"""

import logging
import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
log = logging.getLogger("cdc_to_iceberg")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_PREFIX = os.environ.get("CDC_TOPIC_PREFIX", "shop.ecommerce")
CHECKPOINT_LOCATION = os.environ.get("CHECKPOINT_LOCATION", "/opt/spark/checkpoints/cdc_to_iceberg")
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "10 seconds")


def col(name, data_type=None):
    return StructField(name, data_type or StringType())


# Row structure of each source table as sent by Debezium (JSON, schemas disabled).
# NUMERIC arrives as string (decimal.handling.mode=string), TIMESTAMPTZ as ISO-8601 string.
TABLES = {
    "customers": {
        "pk": "customer_id",
        "columns": [
            col("customer_id", LongType()),
            col("email"),
            col("first_name"),
            col("last_name"),
            col("city"),
            col("country"),
            col("created_at"),
            col("updated_at"),
        ],
    },
    "products": {
        "pk": "product_id",
        "columns": [
            col("product_id", LongType()),
            col("sku"),
            col("name"),
            col("category"),
            col("price"),
            col("is_active", BooleanType()),
            col("created_at"),
            col("updated_at"),
        ],
    },
    "orders": {
        "pk": "order_id",
        "columns": [
            col("order_id", LongType()),
            col("customer_id", LongType()),
            col("status"),
            col("created_at"),
            col("updated_at"),
        ],
    },
    "order_items": {
        "pk": "order_item_id",
        "columns": [
            col("order_item_id", LongType()),
            col("order_id", LongType()),
            col("product_id", LongType()),
            col("quantity", IntegerType()),
            col("unit_price"),
            col("created_at"),
            col("updated_at"),
        ],
    },
}

# Target types in silver tables for columns Debezium sends as strings
SILVER_TYPES = {
    "price": "DECIMAL(10,2)",
    "unit_price": "DECIMAL(10,2)",
    "created_at": "TIMESTAMP",
    "updated_at": "TIMESTAMP",
}

CDC_METADATA_DDL = "_cdc_op STRING, _cdc_lsn BIGINT, _cdc_event_ts TIMESTAMP, _ingested_at TIMESTAMP"


def envelope_schema(columns):
    row = StructType(columns)
    return StructType(
        [
            StructField("before", row),
            StructField("after", row),
            StructField("op", StringType()),
            StructField("ts_ms", LongType()),
            StructField(
                "source",
                StructType(
                    [
                        StructField("lsn", LongType()),
                        StructField("txId", LongType()),
                        StructField("ts_ms", LongType()),
                    ]
                ),
            ),
        ]
    )


# ---------------------------------------------------------------------------
# Table setup
# ---------------------------------------------------------------------------

def ensure_tables(spark: SparkSession):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS lakehouse.bronze.cdc_events (
            kafka_topic     STRING,
            kafka_partition INT,
            kafka_offset    BIGINT,
            source_table    STRING,
            op              STRING,
            source_lsn      BIGINT,
            source_tx_id    BIGINT,
            event_ts        TIMESTAMP,
            record_key      STRING,
            payload         STRING,
            ingested_at     TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (source_table, days(ingested_at))
        """
    )

    for table, cfg in TABLES.items():
        columns_ddl = ", ".join(
            f"{c.name} {SILVER_TYPES.get(c.name, c.dataType.simpleString())}" for c in cfg["columns"]
        )
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS lakehouse.silver.{table} (
                {columns_ddl}, {CDC_METADATA_DDL}
            )
            USING iceberg
            """
        )
    log.info("Bronze and silver tables are ready")


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------

def to_events(batch_df: DataFrame) -> DataFrame:
    """Kafka records -> generic CDC events (one row per change, raw JSON kept)."""
    raw = batch_df.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("key").cast("string").alias("record_key"),
        F.col("value").cast("string").alias("payload"),
    ).filter(F.col("payload").isNotNull())

    return raw.select(
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        F.regexp_extract("kafka_topic", r"([^.]+)$", 1).alias("source_table"),
        F.get_json_object("payload", "$.op").alias("op"),
        F.get_json_object("payload", "$.source.lsn").cast("bigint").alias("source_lsn"),
        F.get_json_object("payload", "$.source.txId").cast("bigint").alias("source_tx_id"),
        F.expr("timestamp_millis(cast(get_json_object(payload, '$.source.ts_ms') AS BIGINT))").alias("event_ts"),
        "record_key",
        "payload",
        F.current_timestamp().alias("ingested_at"),
    )


def merge_bronze(spark: SparkSession):
    # Idempotent append: a replayed batch does not create duplicates
    spark.sql(
        """
        MERGE INTO lakehouse.bronze.cdc_events t
        USING batch_events s
        ON  t.kafka_topic = s.kafka_topic
        AND t.kafka_partition = s.kafka_partition
        AND t.kafka_offset = s.kafka_offset
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def merge_silver(spark: SparkSession, events: DataFrame, table: str, cfg: dict):
    pk = cfg["pk"]
    table_events = events.filter(F.col("source_table") == table)
    if table_events.isEmpty():
        return

    parsed = table_events.select(
        "kafka_offset",
        F.from_json("payload", envelope_schema(cfg["columns"])).alias("e"),
    )

    columns = []
    for c in cfg["columns"]:
        if c.name == pk:
            # For deletes 'after' is null - the key comes from 'before'
            expr = F.coalesce(F.col(f"e.after.{pk}"), F.col(f"e.before.{pk}"))
        else:
            expr = F.col(f"e.after.{c.name}")
        if c.name in SILVER_TYPES:
            expr = expr.cast(SILVER_TYPES[c.name])
        columns.append(expr.alias(c.name))

    rows = parsed.select(
        "kafka_offset",
        *columns,
        F.col("e.op").alias("_cdc_op"),
        F.col("e.source.lsn").alias("_cdc_lsn"),
        F.expr("timestamp_millis(e.source.ts_ms)").alias("_cdc_event_ts"),
        F.current_timestamp().alias("_ingested_at"),
    )

    # The same row can change several times within one micro-batch:
    # keep only its latest version (highest LSN, then highest Kafka offset)
    latest_first = Window.partitionBy(pk).orderBy(F.col("_cdc_lsn").desc(), F.col("kafka_offset").desc())
    latest = (
        rows.withColumn("_rn", F.row_number().over(latest_first))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "kafka_offset")
    )

    view = f"batch_{table}"
    latest.createOrReplaceTempView(view)

    # LSN guard: never overwrite a newer state with an older event
    spark.sql(
        f"""
        MERGE INTO lakehouse.silver.{table} t
        USING {view} s
        ON t.{pk} = s.{pk}
        WHEN MATCHED AND s._cdc_op = 'd'
             AND coalesce(s._cdc_lsn, 0) >= coalesce(t._cdc_lsn, 0) THEN DELETE
        WHEN MATCHED AND s._cdc_op <> 'd'
             AND coalesce(s._cdc_lsn, 0) >= coalesce(t._cdc_lsn, 0) THEN UPDATE SET *
        WHEN NOT MATCHED AND s._cdc_op <> 'd' THEN INSERT *
        """
    )


def process_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.isEmpty():
        return

    spark = batch_df.sparkSession
    events = to_events(batch_df).persist()
    try:
        events.createOrReplaceTempView("batch_events")
        merge_bronze(spark)

        for table, cfg in TABLES.items():
            merge_silver(spark, events, table, cfg)

        summary = ", ".join(
            f"{r['source_table']}.{r['op']}={r['count']}"
            for r in events.groupBy("source_table", "op").count().orderBy("source_table", "op").collect()
        )
        log.info("Batch %s processed: %s", batch_id, summary)
    finally:
        events.unpersist()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    spark = SparkSession.builder.appName("cdc-to-iceberg").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ensure_tables(spark)

    topics = ",".join(f"{TOPIC_PREFIX}.{table}" for table in TABLES)
    log.info("Subscribing to topics: %s", topics)

    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topics)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 10000)
        .load()
    )

    query = (
        stream.writeStream.queryName("cdc_to_iceberg")
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    log.info("Streaming query started, checkpoint: %s", CHECKPOINT_LOCATION)
    query.awaitTermination()


if __name__ == "__main__":
    main()
