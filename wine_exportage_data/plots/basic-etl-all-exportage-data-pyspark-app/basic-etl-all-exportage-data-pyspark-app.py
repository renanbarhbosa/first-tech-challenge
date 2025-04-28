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
        "C:\\intellij-projects\\spark-specialization\\wine_exportage_data\\table_wine_exportage_data\\*",
        schema=explicit_schema,
        header=True,
    )
    df_grape_juice = spark.read.csv(
        "C:\\intellij-projects\\spark-specialization\\wine_exportage_data\\grape_juice_exportage_data\\*",
        schema=explicit_schema,
        header=True,
    )
    df_fresh_grapes = spark.read.csv(
        "C:\\intellij-projects\\spark-specialization\\wine_exportage_data\\fresh_grapes_exportage_data\\*",
        schema=explicit_schema,
        header=True,
    )
    df_foaming_wine = spark.read.csv(
        "C:\\intellij-projects\\spark-specialization\\wine_exportage_data\\foaming_wine_exportage_data\\*",
        schema=explicit_schema,
        header=True,
    )

    # Unir DataFrames
    df_full_export = (
        df_table_wine
        .unionByName(df_foaming_wine)
        .unionByName(df_grape_juice)
        .unionByName(df_fresh_grapes)
    )

    # Função para tratar e converter dados
    def cast_and_clean(df):
        return (
            df
            # Remover pontos (separador de milhar) e converter para DoubleType
            .withColumn(
                "Quantidade (Kg)",
                F.regexp_replace(F.col("Quantidade (Kg)"), "[.]", "").cast(DoubleType())
            )
            .withColumn(
                "Valor (US$)",
                F.regexp_replace(F.col("Valor (US$)"), "[.]", "").cast(DoubleType())
            )
            # Remover linhas inválidas
            .dropna(subset=["Quantidade (Kg)", "Valor (US$)"])
        )

    # Aplicar transformação ao DataFrame completo
    df_full_export_clean = cast_and_clean(df_full_export)

    # Agrupar por país e somar valores
    df_consolidado = df_full_export_clean.groupBy("Países").agg(
        F.sum("Quantidade (Kg)").alias("Quantidade Total (Kg)"),
        F.sum("Valor (US$)").alias("Valor Total (US$)")
    )

    # Ordenar por valor total e selecionar top 10
    df_top_countries = df_consolidado.orderBy(F.desc("Valor Total (US$)")).limit(10)

    # Verificar dados (DEBUG)
    print("=== Dados Consolidados ===")
    df_top_countries.show(10, truncate=False)

    # Converter para Pandas e plotar
    df_top_countries_ready_to_plot = df_top_countries.toPandas()

    plt.figure(figsize=(18, 8))
    sns.barplot(
        data=df_top_countries_ready_to_plot,
        x='Países',
        y='Valor Total (US$)',
        color='blue'
    )

    plt.title('Top Países por Valor Total Exportado (US$)', fontsize=14)
    plt.xlabel('Países', fontsize=12)
    plt.ylabel('Valor Total (US$)', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    # Adicionar valores nas barras
    for index, value in enumerate(df_top_countries_ready_to_plot['Valor Total (US$)']):
        plt.text(
            index,
            value,
            f'${value:,.0f}',
            ha='center',
            va='bottom',
            rotation=45,
            fontsize=8
        )

    plt.tight_layout()
    plt.show()