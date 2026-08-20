from pyspark.sql.functions import col, explode, current_timestamp, from_unixtime, from_json
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, DoubleType, LongType
import dlt

catalog_name = 'youtube_dev'
volume_path = f'/Volumes/{catalog_name}/bronze/earthquake_data'
primary_key='id'

# Define schema for the features array
features_schema = ArrayType(StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("properties", StructType([
        StructField("mag", DoubleType(), True),
        StructField("place", StringType(), True),
        StructField("time", LongType(), True),
        StructField("updated", LongType(), True),
        StructField("tz", StringType(), True),
        StructField("url", StringType(), True),
        StructField("detail", StringType(), True),
        StructField("felt", LongType(), True),
        StructField("cdi", DoubleType(), True),
        StructField("mmi", DoubleType(), True),
        StructField("alert", StringType(), True),
        StructField("status", StringType(), True),
        StructField("tsunami", LongType(), True),
        StructField("sig", LongType(), True),
        StructField("net", StringType(), True),
        StructField("code", StringType(), True),
        StructField("ids", StringType(), True),
        StructField("sources", StringType(), True),
        StructField("types", StringType(), True),
        StructField("nst", LongType(), True),
        StructField("dmin", DoubleType(), True),
        StructField("rms", DoubleType(), True),
        StructField("gap", DoubleType(), True),
        StructField("magType", StringType(), True)
    ]), True),
    StructField("geometry", StructType([
        StructField("type", StringType(), True),
        StructField("coordinates", ArrayType(DoubleType(), True), True)
    ]), True)
]))

@dlt.view(name='earthquake_data_vw')
def earthquake_data():
    df = spark.readStream.format('cloudFiles') \
        .option('cloudFiles.format', 'json') \
        .load(volume_path) \
        .withColumn('_load_timestamp', current_timestamp())
    # Parse the features string column as JSON
    df = df.withColumn('parsed_features', from_json(col('features'), features_schema))
    df = df.select('_load_timestamp', explode(col('parsed_features')).alias('feature'))
    df.printSchema()
    df = df.select(
        '_load_timestamp',
        'feature.properties.*',
        'feature.id',
        col('feature.geometry.coordinates').getItem(0).alias('longitude'),
        col('feature.geometry.coordinates').getItem(1).alias('latitude'),
        col('feature.geometry.coordinates').getItem(2).alias('depth')
    )
    df = df.withColumn('time', from_unixtime(col('time')/1000).cast('timestamp'))
    
    return df

# Create the target streaming table and apply CDC
dlt.create_streaming_table(name='earthquake_data_clean', comment='Earthquake data')
dlt.apply_changes(
    target='earthquake_data_clean',
    source='earthquake_data_vw',
    keys=[primary_key],
    sequence_by='_load_timestamp',
    stored_as_scd_type='1'
)