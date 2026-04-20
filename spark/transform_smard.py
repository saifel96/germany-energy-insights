from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, explode, year , month, input_file_name, regexp_extract, when, to_timestamp

spark = SparkSession.builder \
    .appName("SMARD Transform") \
    .getOrCreate()

print("Spark läuft!")


def smardProcess(data_path : str ):

    df_raw = spark.read.json(f"{data_path}/*.json").withColumn('file_path', input_file_name())
    
    df_filter_id = df_raw.withColumn('filter_id', regexp_extract('file_path', r"(\d+)_DE-LU", 1))

    df_exploded = df_filter_id.select('filter_id', explode('series').alias('entry'))

    df_new = df_exploded.select(col('filter_id') ,from_unixtime(col('entry')[0] / 1000.0).alias('timeseries'), 
                                   col('entry')[1].cast('double').alias('value')
                                ).withColumn("category",
                                when(col('filter_id') == '1223', "Braunkohle")
                                .when(col('filter_id') == '1224', "Kernenergie")
                                .when(col('filter_id') == '1225', "Wind Offshore")
                                .when(col('filter_id') == '1226', "Wasserkraft")
                                .when(col('filter_id') == '1227', "Sonstige Konventionelle")
                                .when(col('filter_id') == '1228', "Sonstige Erneuerbare")
                                .when(col('filter_id') == '4066', "Biomasse")
                                .when(col('filter_id') == '4067', "Wind Onshore")
                                .when(col('filter_id') == '4068', "Photovoltaik")
                                .when(col('filter_id') == '4069', "Steinkohle")
                                .when(col('filter_id') == '4070', "Pumpspeicher")
                                .when(col('filter_id') == '4071', "Erdgas")
                                .when(col('filter_id') == '410', "Gesamt (Netzlast)")
                                .when(col('filter_id') == '4359', "Residuallast")
                                .when(col('filter_id') == '4387', "Pumpspeicher")
                                .otherwise("Other")
                                )

    df_new.show(5)

    df_final = df_new.withColumns({"month": month(to_timestamp(col('timeseries'))),
                                 "year": year(to_timestamp(col('timeseries')))}
                                 )
    df_final.show(5)

    df_final.write \
    .partitionBy("year", "month") \
    .mode("overwrite") \
    .parquet("data/processed/")

if __name__ == "__main__":
    data_path = "data" 
    smardProcess(data_path)