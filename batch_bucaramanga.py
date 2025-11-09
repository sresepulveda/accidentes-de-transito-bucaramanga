from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, count
#Codigo creado por JeffersonRemolina para UNAD-BigData
def main():
    spark = SparkSession.builder \
        .appName("accidentesDeTransitoEnBucaramanga") \
        .getOrCreate()

    # 1. Cargar archivo CSV
    df = spark.read.csv("transito.csv", header=True, sep=";")

    # 2. Limpieza: quitar los espacios y filas vacías
    df = df.select([trim(col(c)).alias(c) for c in df.columns])
    df = df.dropDuplicates()
    df = df.dropna(how="all")

    # Análisis exploratorio (EDA)

    # 1) Accidentes por gravedad
    df_gravedad = df.groupBy("GRAVEDAD").agg(count("*").alias("Total_Accidentes"))

    # 2) Accidentes por tipo de vehículo motocicleta (si MOTO es numérica)
    df_motos = df.groupBy("MOTO").agg(count("*").alias("Cantidad"))

    # 3) Accidentes por barrio
    df_barrios = df.groupBy("BARRIO").agg(count("*").alias("Accidentes"))

    # Guardar resultados en un solo CSV
    df_gravedad.coalesce(1).write.option("header", True).mode("overwrite").csv("resultados_csv/gravedad")
    df_motos.coalesce(1).write.option("header", True).mode("overwrite").csv("resultados_csv/motos")
    df_barrios.coalesce(1).write.option("header", True).mode("overwrite").csv("resultados_csv/barrios")

    print("Procesamiento completado. Los resultados seran guardados en carpetas CSV.")

    spark.stop()

if __name__ == "__main__":
    main()


