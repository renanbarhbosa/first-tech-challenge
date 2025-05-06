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

base_path = "C:/intellij-projects/postech/first-tech-challenge/wine_import_data/files"

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
    spark = SparkSession.builder.appName("advanced-import-analysis").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Ler e combinar dados de importação
    products = ["foaming_wine", "fresh_grapes", "grape_juice", "table_wine", "raisins_grapes"]
    df_full_import = read_product_data(spark, products[0])

    for product in products[1:]:
        df = read_product_data(spark, product)
        df_full_import = df_full_import.unionByName(df)

    # Processamento de dados
    df_clean = df_full_import.transform(lambda df:
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

    # Filtrar dados apenas para o País
    country_data = pd_df[pd_df['Países'] == 'Chile'].sort_values('Year')

    if country_data.empty:
        print("Nenhum dado encontrado para o país.")
        spark.stop()
        exit()

    # Criar diretório para análise do país
    os.makedirs("chile_import_analise", exist_ok=True)

    # Configurações de visualização
    sns.set(style="whitegrid", font="DejaVu Sans")
    plt.rcParams['axes.prop_cycle'] = plt.cycler(color=plt.cm.Dark2.colors)

    # Função para formatar eixos
    def formatar_eixos(ax, tipo):
        if tipo == 'valor':
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: formatar_valor(x)))
        else:
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: formatar_quantidade(x)))
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()

    # Gráfico de Evolução do Valor
    plt.figure(figsize=(14, 7))
    ax = sns.lineplot(
        data=country_data,
        x='Year',
        y='Valor Total (US$)',
        marker='o',
        markersize=8,
        linewidth=2.5
    )

    # Adicionar valores nos pontos
    for year, valor in zip(country_data['Year'], country_data['Valor Total (US$)']):
        plt.text(
            year,
            valor * 1.05,
            formatar_valor(valor),
            ha='center',
            va='bottom',
            fontsize=9
        )

    plt.title('Evolução das Importações de Vinho da Chile (Valor)', fontsize=16, pad=20)  # Alterado
    plt.xlabel('Ano', fontsize=12)
    plt.ylabel('Valor Total (US$)', fontsize=12)
    formatar_eixos(ax, 'valor')
    plt.savefig("chile_import_analise/evolucao_valor_import_chile.png", dpi=300, bbox_inches='tight')  # Alterado
    plt.close()

    # Gráfico de Evolução da Quantidade
    plt.figure(figsize=(14, 7))
    ax = sns.barplot(
        data=country_data,
        x='Year',
        y='Quantidade Total (Kg)',
        palette='Blues_d'
    )

    # Adicionar valores nas barras
    for p in ax.patches:
        ax.annotate(
            formatar_quantidade(p.get_height()),
            (p.get_x() + p.get_width() / 2., p.get_height()),
            ha='center',
            va='center',
            xytext=(0, 10),
            textcoords='offset points',
            fontsize=9
        )

    plt.title('Evolução das Importações de Vinho da Chile (Quantidade)', fontsize=16, pad=20)  # Alterado
    plt.xlabel('Ano', fontsize=12)
    plt.ylabel('Quantidade Total (Kg)', fontsize=12)
    formatar_eixos(ax, 'quantidade')
    plt.savefig("chile_import_analise/evolucao_quantidade_import_chile.png", dpi=300, bbox_inches='tight')  # Alterado
    plt.close()

    spark.stop()