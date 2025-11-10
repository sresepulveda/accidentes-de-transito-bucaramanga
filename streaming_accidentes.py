from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
#Codigo creado por JeffersonRemolina para UNAD-BigData
def main():
    spark = SparkSession.builder \
        .appName("StreamingDeAccidentesEnBucaramanga") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # esto permite leer flujo de Kafka
    df_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "accidentes_stream") \
        .load()

    # aca de convierte el valor del mensaje a texto
    df_text = df_stream.selectExpr("CAST(value AS STRING) as evento")

    # dividimos el evento en columnas
    columnas = split(col("evento"), ";")

    df_eventos = df_text.select(
        columnas.getItem(0).alias("TIPO"),
        columnas.getItem(1).alias("GRAVEDAD"),
        columnas.getItem(2).alias("VEHICULO"),
        columnas.getItem(3).alias("BARRIO")
    )

    # analizamos en tiempo real: contar accidentes por vehículo
    conteo = df_eventos.groupBy("VEHICULO").count()

    # aca mostramos los resultados en la consola en tiempo real
    query = conteo.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
