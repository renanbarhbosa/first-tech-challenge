from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import matplotlib.pyplot as plt
import seaborn as sns

# Definir schema explícito
explicit_schema = StructType(
    [
        StructField("Países", StringType(), True),
        StructField("Quantidade (Kg)", StringType(), True),
        StructField("Valor (US$)", StringType(), True),
    ]
)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("basic-etl-pyspark-app").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Ler dados
    df_table_wine = spark.read.csv(
        "C:\\intellij-projects\\postech\\first-tech-challenge\\wine_import_data\\files\\table_wine_import_data\\*",
        schema=explicit_schema,
        header=True,
    )
    df_raisins_grapes = spark.read.csv(
        "C:\\intellij-projects\\postech\\first-tech-challenge\\wine_import_data\\files\\raisins_grapes\\*",
        schema=explicit_schema,
        header=True,
    )
    df_grape_juice = spark.read.csv(
        "C:\\intellij-projects\\postech\\first-tech-challenge\\wine_import_data\\files\\grape_juice\\*",
        schema=explicit_schema,
        header=True,
    )
    df_fresh_grapes = spark.read.csv(
        "C:\\intellij-projects\\postech\\first-tech-challenge\\wine_import_data\\files\\fresh_grapes\\*",
        schema=explicit_schema,
        header=True,
    )
    df_foaming_wine = spark.read.csv(
        "C:\\intellij-projects\\postech\\first-tech-challenge\\wine_import_data\\files\\foaming_wine\\*",
        schema=explicit_schema,
        header=True,
    )

    # Unir DataFrames
    df_full_export = (
        df_table_wine.unionByName(df_foaming_wine)
        .unionByName(df_grape_juice)
        .unionByName(df_fresh_grapes)
        .unionByName(df_grape_juice)
        .unionByName(df_raisins_grapes)
    )

    # Função para tratar e converter dados
    def cast_and_clean(df):
        return (
            df
            # Remover pontos (separador de milhar) e converter para DoubleType
            .withColumn(
                "Quantidade (Kg)",
                F.regexp_replace(F.col("Quantidade (Kg)"), "[.]", "").cast(
                    DoubleType()
                ),
            ).withColumn(
                "Valor (US$)",
                F.regexp_replace(F.col("Valor (US$)"), "[.]", "").cast(DoubleType()),
            )
            # Remover linhas inválidas
            .dropna(subset=["Quantidade (Kg)", "Valor (US$)"])
        )

    # Aplicar transformação ao DataFrame completo
    df_full_export_clean = cast_and_clean(df_full_export)

    #TODO plotar os graficos
    