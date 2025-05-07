from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Configurações
base_path = "C:/intellij-projects/postech/first-tech-challenge/wine_exportage_data/files"
output_dir = r"C:\intellij-projects\postech\first-tech-challenge\argentina-micro-analysis"

# Schema para ler dados brutos como string única
raw_schema = StructType([
    StructField("raw_data", StringType(), True)
])


def read_product_data(spark, product_name):
    """Lê dados considerando formato irregular com encoding correto"""
    return (
        spark.read.csv(
            f"{base_path}/{product_name}/*.csv",
            schema=raw_schema,
            header=True,
            sep="\n",  # Ler cada linha como registro único
            encoding="ISO-8859-1"  # Encoding comum para dados brasileiros
        )
        .withColumn(
            "Year",
            F.regexp_extract(F.input_file_name(), r"dados_(\d{4})\.csv", 1).cast(IntegerType())
        )
    )


def process_raw_data(df):
    """Processa a coluna raw_data para extrair campos"""
    return df.withColumn(
        "temp_split", F.split(F.col("raw_data"), ",")
    ).select(
        F.col("Year"),
        F.trim(F.col("temp_split").getItem(0)).alias("País"),
        F.regexp_replace(F.col("temp_split").getItem(1), "[^0-9,]", "").alias("Quantidade"),
        F.regexp_replace(F.col("temp_split").getItem(2), "[^0-9,]", "").alias("Valor")
    ).drop("temp_split", "raw_data")


def convert_columns(df):
    """Converte colunas numéricas com tratamento de decimais"""
    return df.withColumn(
        "Quantidade",
        F.regexp_replace(F.col("Quantidade"), ",", ".").cast(DoubleType())
    ).withColumn(
        "Valor",
        F.regexp_replace(F.col("Valor"), ",", ".").cast(DoubleType())
    ).filter(
        (F.lower(F.col("País")).contains("argentina")) &
        (F.col("Quantidade") > 0) &
        (F.col("Valor") > 0)
    )


def format_currency(value):
    """Formata valores monetários de forma inteligente"""
    if value >= 1e9:
        return f"${value / 1e9:.2f}B"
    elif value >= 1e6:
        return f"${value / 1e6:.2f}M"
    elif value >= 1e3:
        return f"${value / 1e3:.1f}K"
    return f"${value:.2f}"


def format_quantity(value):
    """Formata quantidades de forma legível"""
    if value >= 1e6:
        return f"{value / 1e6:.2f}M Kg"
    elif value >= 1e3:
        return f"{value / 1e3:.1f}K Kg"
    return f"{value:.0f} Kg"


def save_plot(data, x, y, title, filename, plot_type='line'):
    """Salva gráficos com formatação consistente"""
    plt.figure(figsize=(14, 7))

    if plot_type == 'line':
        ax = sns.lineplot(data=data, x=x, y=y, marker='o', linewidth=2.5)
    else:
        ax = sns.barplot(data=data, x=x, y=y, palette='Blues_d')

    plt.title(title, fontsize=16)
    plt.xlabel('Ano', fontsize=12)
    plt.xticks(rotation=45)

    # Formatar eixos
    if 'Valor' in y:
        ax.yaxis.set_major_formatter(lambda x, _: format_currency(x))
    else:
        ax.yaxis.set_major_formatter(lambda x, _: format_quantity(x))

    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, filename), dpi=300, bbox_inches='tight')
    plt.close()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("ArgentinaWineExports").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        products = ["foaming_wine", "table_wine"]
        all_data = []

        for product in products:
            print(f"\nProcessando {product}...")

            # Ler e processar dados
            raw_df = read_product_data(spark, product)
            processed_df = process_raw_data(raw_df)
            final_df = convert_columns(processed_df)

            print(f"Exemplo de dados processados para {product}:")
            final_df.show(5, truncate=False)

            all_data.append(final_df)

        # Combinar todos os dados
        combined_df = all_data[0]
        for df in all_data[1:]:
            combined_df = combined_df.unionByName(df)

        if combined_df.isEmpty():
            raise ValueError("Nenhum dado válido encontrado após o processamento")

        # Agregar por ano
        annual_data = combined_df.groupBy("Year").agg(
            F.sum("Quantidade").alias("Total_Quantidade"),
            F.sum("Valor").alias("Total_Valor")
        ).orderBy("Year")

        print("\nDados agregados por ano:")
        annual_data.show(truncate=False)

        # Converter para Pandas
        pd_df = annual_data.toPandas()

        # Criar diretório de saída
        os.makedirs(output_dir, exist_ok=True)

        # Gerar gráficos
        save_plot(pd_df, 'Year', 'Total_Valor',
                  'Evolução do Valor das Exportações Vinícolas Argentinas',
                  'vinhos/exportacoes_vinhos_valor.png', 'line')

        save_plot(pd_df, 'Year', 'Total_Quantidade',
                  'Evolução da Quantidade Exportada de Vinhos Argentinos',
                  'vinhos/exportacoes_vinhos_quantidade.png', 'bar')

        print(f"\nProcesso concluído! Gráficos salvos em: {output_dir}")

    except Exception as e:
        print(f"\nErro durante a execução: {str(e)}")
        import traceback

        traceback.print_exc()

    finally:
        spark.stop()