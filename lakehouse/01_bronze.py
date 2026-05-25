"""
Bronze Layer: Raw Data Ingestion
Membaca data dari HDFS, menambahkan metadata, dan menyimpan ke Delta Lake format
"""

import logging
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from delta import configure_spark_with_delta_pip

# Configuration
HDFS_NAMENODE = "localhost"
HDFS_PORT = "9000"
BRONZE_OUTPUT_DIR = "./lakehouse_data/bronze"
LOGS_DIR = "./logs"

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(f'{LOGS_DIR}/lakehouse_bronze.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Initialize Spark with Delta Lake support"""
    try:
        logger.info("Initializing Spark session dengan Delta Lake...")
        builder = SparkSession.builder \
            .appName("Bronze-Pangan-Lakehouse") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g")
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info("✓ Spark session berhasil dibuat")
        return spark
    except Exception as e:
        logger.error(f"Gagal membuat Spark session: {str(e)}")
        raise


def read_json_from_hdfs(spark, hdfs_path, source_name):
    """Read JSON data dari HDFS dengan error handling"""
    try:
        logger.info(f"Membaca data {source_name} dari HDFS: {hdfs_path}...")
        
        df = spark.read \
            .option("multiLine", True) \
            .option("mode", "PERMISSIVE") \
            .json(hdfs_path)
        
        row_count = df.count()
        logger.info(f"✓ Berhasil membaca {row_count:,} records dari {source_name}")
        
        return df
        
    except Exception as e:
        logger.error(f"Gagal membaca {source_name} dari HDFS: {str(e)}")
        raise


def add_metadata(df, source_name):
    """Tambahkan metadata columns (timestamp dan source)"""
    try:
        logger.info(f"Menambahkan metadata untuk {source_name}...")
        
        df = df \
            .withColumn("_ingested_at", current_timestamp()) \
            .withColumn("_source", lit(source_name))
        
        logger.info(f"✓ Metadata ditambahkan. Total columns: {len(df.columns)}")
        
        return df
        
    except Exception as e:
        logger.error(f"Gagal menambahkan metadata: {str(e)}")
        raise


def write_to_delta_bronze(df, output_path, source_name):
    """Write data ke Delta Lake format (append mode)"""
    try:
        logger.info(f"Menulis {source_name} data ke Bronze layer: {output_path}...")
        
        df.write \
            .format("delta") \
            .mode("append") \
            .save(output_path)
        
        logger.info(f"✓ Berhasil menulis ke {output_path}")
        
    except Exception as e:
        logger.error(f"Gagal menulis {source_name} ke Delta: {str(e)}")
        raise


def verify_bronze_layer(spark, output_path, source_name):
    """Verify data yang tersimpan di Bronze layer"""
    try:
        logger.info(f"Verifying Bronze {source_name} layer...")
        
        df = spark.read.format("delta").load(output_path)
        
        # Display schema
        logger.info(f"Schema ({source_name}):")
        df.printSchema()
        
        # Count records
        row_count = df.count()
        logger.info(f"Total records: {row_count:,}")
        
        # Check metadata columns
        if "_ingested_at" in df.columns and "_source" in df.columns:
            logger.info("✓ Metadata columns verified")
        
        logger.info(f"✓ Verification {source_name} complete")
        
    except Exception as e:
        logger.error(f"Verification gagal untuk {source_name}: {str(e)}")


def main():
    """Execute Bronze layer ingestion pipeline"""
    spark = None
    try:
        spark = create_spark_session()
        
        # ===== API DATA PIPELINE =====
        logger.info("\n" + "="*60)
        logger.info("Proses Data API")
        logger.info("="*60)
        
        api_hdfs_path = f"hdfs://{HDFS_NAMENODE}:{HDFS_PORT}/data/pangan/api/"
        api_bronze_path = f"{BRONZE_OUTPUT_DIR}/pangan_api"
        
        api_df = read_json_from_hdfs(spark, api_hdfs_path, "API")
        api_df = add_metadata(api_df, "api")
        write_to_delta_bronze(api_df, api_bronze_path, "API")
        verify_bronze_layer(spark, api_bronze_path, "API")
        
        # ===== RSS DATA PIPELINE =====
        logger.info("\n" + "="*60)
        logger.info("Proses Data RSS")
        logger.info("="*60)
        
        rss_hdfs_path = f"hdfs://{HDFS_NAMENODE}:{HDFS_PORT}/data/pangan/rss/"
        rss_bronze_path = f"{BRONZE_OUTPUT_DIR}/pangan_rss"
        
        rss_df = read_json_from_hdfs(spark, rss_hdfs_path, "RSS")
        rss_df = add_metadata(rss_df, "rss")
        write_to_delta_bronze(rss_df, rss_bronze_path, "RSS")
        verify_bronze_layer(spark, rss_bronze_path, "RSS")
        
        # ===== SUMMARY =====
        logger.info("\n" + "="*60)
        logger.info("BRONZE LAYER INGESTION SUMMARY")
        logger.info("="*60)
        logger.info("  API: ✓ SUCCESS")
        logger.info("  RSS: ✓ SUCCESS")
        logger.info("✓ Bronze layer ingestion berhasil!")
        
    except Exception as e:
        logger.error(f"Bronze layer pipeline gagal: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session ditutup")


if __name__ == "__main__":
    main()
