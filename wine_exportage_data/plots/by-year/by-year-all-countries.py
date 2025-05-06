from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Configurar schema explícito
explicit_schema = StructType([
    StructField("Países", StringType(), True),
    StructField("Quantidade (Kg)", StringType(), True),
    StructField("Valor (US$)", StringType(), True)
])

base_path = "C:/intellij-projects/postech/first-tech-challenge/wine_exportage_data/files"


def read_product_data(spark, product_name):
    """Lê e processa dados de um produto específico com extração do ano"""
    return (
        spark.read.csv(
            f"{base_path}/{product_name}/*.csv",
            schema=explicit_schema,
            header=True
        )
        .withColumn(
            "Year",
            F.regexp_extract(F.input_file_name(), r"dados_(\d{4})\.csv", 1).cast(IntegerType())
        )
    )


def formatar_valor(valor):
    """Formata valores para escala de milhões/bilhões"""
    if valor >= 1e9:
        return f"${valor / 1e9:,.1f}B"
    return f"${valor / 1e6:,.0f}M"


def formatar_quantidade(quantidade):
    """Formata quantidades para escala de milhões/bilhões"""
    if quantidade >= 1e9:
        return f"{quantidade / 1e9:,.1f}B Kg"
    return f"{quantidade / 1e6:,.0f}M Kg"


if __name__ == "__main__":
    spark = SparkSession.builder.appName("advanced-export-analysis").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Ler e combinar dados
    products = ["foaming_wine", "fresh_grapes", "grape_juice", "table_wine"]
    df_full_export = read_product_data(spark, products[0])

    for product in products[1:]:
        df = read_product_data(spark, product)
        df_full_export = df_full_export.unionByName(df)

    # Processamento de dados
    df_clean = df_full_export.transform(lambda df:
                                        df.withColumn("Quantidade (Kg)",
                                                      F.regexp_replace(F.col("Quantidade (Kg)"), "[.]", "").cast(
                                                          DoubleType()))
                                        .withColumn("Valor (US$)",
                                                    F.regexp_replace(F.col("Valor (US$)"), "[.]", "").cast(
                                                        DoubleType()))
                                        .dropna(subset=["Quantidade (Kg)", "Valor (US$)"])
                                        .filter(F.col("Países") != "Total")
                                        )

    df_annual = df_clean.groupBy("Year", "Países").agg(
        F.sum("Quantidade (Kg)").alias("Quantidade Total (Kg)"),
        F.sum("Valor (US$)").alias("Valor Total (US$)")
    )

    # Converter para Pandas e ordenar
    pd_df = df_annual.toPandas().sort_values(["Year", "Valor Total (US$)"], ascending=[True, False])

    # Configurações de visualização
    sns.set(style="whitegrid", font="DejaVu Sans")
    plt.rcParams['axes.prop_cycle'] = plt.cycler(color=plt.cm.tab20.colors)

    # Criar diretórios
    os.makedirs("valor_anual", exist_ok=True)
    os.makedirs("quantidade_anual", exist_ok=True)

    # Gerar gráficos anuais
    for year in sorted(pd_df['Year'].unique()):
        year_data = pd_df[pd_df['Year'] == year]

        # Gráfico de Valor
        plt.figure(figsize=(16, 10))
        top_valor = year_data.nlargest(15, 'Valor Total (US$)')

        ax = sns.barplot(
            x='Valor Total (US$)',
            y='Países',
            hue='Países',  # Correção aplicada
            data=top_valor,
            palette="tab20",
            legend=False,
            edgecolor='black',
            linewidth=0.7
        )

        # Adicionar valores
        for i, valor in enumerate(top_valor['Valor Total (US$)']):
            plt.text(valor + valor * 0.01, i,
                     formatar_valor(valor),
                     va='center',
                     fontsize=10,
                     color='navy')

        plt.title(f'TOP 15 Países por Valor Exportado - {year}', fontsize=18, pad=20)
        plt.xlabel('Valor Total (US$)', fontsize=14)
        plt.ylabel('')
        plt.xlim(0, top_valor['Valor Total (US$)'].max() * 1.2)
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: formatar_valor(x)))

        plt.savefig(f"valor_anual/{year}_valor.png", dpi=300, bbox_inches='tight')
        plt.close()

        # Gráfico de Quantidade
        plt.figure(figsize=(16, 10))
        top_quantidade = year_data.nlargest(15, 'Quantidade Total (Kg)')

        ax = sns.barplot(
            x='Quantidade Total (Kg)',
            y='Países',
            hue='Países',  # Correção aplicada
            data=top_quantidade,
            palette="tab20",
            legend=False,
            edgecolor='black',
            linewidth=0.7
        )

        # Adicionar valores
        for i, quantidade in enumerate(top_quantidade['Quantidade Total (Kg)']):
            plt.text(quantidade + quantidade * 0.01, i,
                     formatar_quantidade(quantidade),
                     va='center',
                     fontsize=10,
                     color='darkgreen')

        plt.title(f'TOP 15 Países por Quantidade Exportada - {year}', fontsize=18, pad=20)
        plt.xlabel('Quantidade Total (Kg)', fontsize=14)
        plt.ylabel('')
        plt.xlim(0, top_quantidade['Quantidade Total (Kg)'].max() * 1.2)
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: formatar_quantidade(x)))

        plt.savefig(f"quantidade_anual/{year}_quantidade.png", dpi=300, bbox_inches='tight')
        plt.close()

    spark.stop()