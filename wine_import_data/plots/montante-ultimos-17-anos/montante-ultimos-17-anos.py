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

    # Unir DataFrames (corrigindo uniões duplicadas)
    df_full_import = (
        df_table_wine
        .unionByName(df_foaming_wine)
        .unionByName(df_grape_juice)
        .unionByName(df_fresh_grapes)
        .unionByName(df_raisins_grapes)  # Removida duplicação do grape_juice
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
    df_full_import_clean = cast_and_clean(df_full_import)

    # Agrupar por país e somar valores
    df_consolidado = (
        df_full_import_clean.groupBy("Países")
        .agg(
            F.sum("Quantidade (Kg)").alias("Quantidade Total (Kg)"),
            F.sum("Valor (US$)").alias("Valor Total (US$)"),
        )
        .filter(F.col("Países") != "Total")
    )

    # =============================================
    # Gráfico para Valor Total de Importação
    # =============================================
    # Ordenar por valor total e selecionar top 30
    df_top_countries = df_consolidado.orderBy(F.desc("Valor Total (US$)")).limit(30)

    # Converter para Pandas e preparar dados
    df_top_countries_ready_to_plot = df_top_countries.toPandas()
    df_top_countries_ready_to_plot = df_top_countries_ready_to_plot.sort_values(
        "Valor Total (US$)", ascending=False
    )

    # Configurar o gráfico horizontal
    plt.figure(figsize=(15, 8))
    sns.barplot(
        data=df_top_countries_ready_to_plot,
        y="Países",
        x="Valor Total (US$)",
        color="blue",
    )

    max_value = df_top_countries_ready_to_plot["Valor Total (US$)"].max()
    plt.xlim(0, max_value * 1.1)
    plt.title("Montante importado dos Países de Origem", fontsize=14)
    plt.xlabel("Valor Total (US$)", fontsize=12)
    plt.ylabel("Países", fontsize=12)
    plt.grid(axis="x", linestyle="--", alpha=0.7)

    # Adicionar valores nas barras
    for index, value in enumerate(df_top_countries_ready_to_plot["Valor Total (US$)"]):
        plt.text(value, index, f"${value:,.0f}", ha="left", va="center", fontsize=8)

    plt.tight_layout()
    plt.show()

    # =============================================
    # Gráfico para Quantidade Total de Importação
    # =============================================
    # Ordenar por quantidade total e selecionar top 30
    df_top_countries_kg = df_consolidado.orderBy(F.desc("Quantidade Total (Kg)")).limit(30)

    # Converter para Pandas e preparar dados
    df_top_countries_kg_ready = df_top_countries_kg.toPandas()
    df_top_countries_kg_ready = df_top_countries_kg_ready.sort_values(
        "Quantidade Total (Kg)", ascending=False
    )

    # Configurar o gráfico de quantidade
    plt.figure(figsize=(15, 8))
    sns.barplot(
        data=df_top_countries_kg_ready,
        y="Países",
        x="Quantidade Total (Kg)",
        color="green"
    )

    max_kg = df_top_countries_kg_ready["Quantidade Total (Kg)"].max()
    plt.xlim(0, max_kg * 1.1)
    plt.title("Quantidade importada dos Países de Origem (Kg)", fontsize=14)
    plt.xlabel("Quantidade Total (Kg)", fontsize=12)
    plt.ylabel("Países", fontsize=12)
    plt.grid(axis="x", linestyle="--", alpha=0.7)

    # Adicionar valores nas barras
    for index, value in enumerate(df_top_countries_kg_ready["Quantidade Total (Kg)"]):
        plt.text(value, index, f"{value:,.0f} Kg", ha="left", va="center", fontsize=8)

    plt.tight_layout()
    plt.show()