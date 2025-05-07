from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Configuração do schema para exportação
schema_exportacao = StructType([
    StructField("Países", StringType(), True),
    StructField("Quantidade (Kg)", StringType(), True),
    StructField("Valor (US$)", StringType(), True)
])

# Configurar caminhos para dados de exportação
caminho_base = "C:/intellij-projects/postech/first-tech-challenge/wine_exportage_data/files"
produtos = ["foaming_wine", "fresh_grapes", "grape_juice", "table_wine"]

def ler_dados_exportacao(spark, produto):
    """Carrega dados de exportação de um produto específico com extração do ano"""
    return (spark.read.csv(
        f"{caminho_base}/{produto}/*.csv",
        schema=schema_exportacao,
        header=True)
            .withColumn("Ano", F.regexp_extract(F.input_file_name(), r"dados_(\d{4})\.csv", 1).cast(IntegerType()))
            )

def formatar_eixo_y(valor, _):
    """Formata valores do eixo Y em milhões/bilhões"""
    if valor >= 1e9:
        return f"${valor / 1e9:.1f}B"
    return f"${valor / 1e6:.0f}M"

def obter_top_paises(df_pandas, top_n=10):
    """Identifica os países com maior valor acumulado"""
    total_por_pais = df_pandas.groupby('Países', as_index=False)['Valor Total (US$)'].sum()
    return total_por_pais.nlargest(top_n, 'Valor Total (US$)')['Países'].tolist()

def configurar_estilo_grafico():
    """Configurações visuais para os gráficos"""
    sns.set(style="whitegrid", rc={
        'figure.figsize': (18, 10),
        'axes.titlesize': 16,
        'axes.labelsize': 14,
        'xtick.labelsize': 12,
        'ytick.labelsize': 12
    })
    plt.rcParams['font.family'] = 'DejaVu Sans'

if __name__ == "__main__":
    # Configurar Spark para exportação
    spark = SparkSession.builder.appName("top-10-exportadores").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Carregar e processar dados de exportação
    df_completo = ler_dados_exportacao(spark, produtos[0])
    for produto in produtos[1:]:
        df_completo = df_completo.unionByName(ler_dados_exportacao(spark, produto))

    # Transformação e limpeza dos dados
    df_limpo = (df_completo
                .withColumn("Valor (US$)", F.regexp_replace(F.col("Valor (US$)"), "[.]", "").cast(DoubleType()))
                .withColumn("Quantidade (Kg)", F.regexp_replace(F.col("Quantidade (Kg)"), "[.]", "").cast(DoubleType()))
                .filter(F.col("Países") != "Total")
                .groupBy("Ano", "Países")
                .agg(F.sum("Valor (US$)").alias("Valor Total (US$)"))
                )

    # Converter para Pandas DataFrame
    pd_df = df_limpo.toPandas().sort_values(["Ano", "Valor Total (US$)"], ascending=[True, False])

    # Identificar top 10 países
    top_paises = obter_top_paises(pd_df)
    df_top = pd_df[pd_df['Países'].isin(top_paises)]

    # Configurar visualização
    configurar_estilo_grafico()
    palette = sns.color_palette("husl", n_colors=10)

    # Criar gráfico de exportação
    plt.figure(dpi=120)
    ax = sns.lineplot(
        data=df_top,
        x="Ano",
        y="Valor Total (US$)",
        hue="Países",
        palette=palette,
        marker="o",
        linewidth=2.5,
        markersize=8,
        estimator=None,
        style="Países",
        dashes=False
    )

    # Personalização do gráfico
    ax.set_title('TOP 10 Países - Evolução Anual do Valor Exportado (US$)', pad=20)
    ax.set_xlabel('Ano', labelpad=15)
    ax.set_ylabel('Valor Total', labelpad=15)
    ax.yaxis.set_major_formatter(formatar_eixo_y)

    # Ajustar legenda
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(
        handles[::-1],
        labels[::-1],
        title="Países",
        bbox_to_anchor=(1.05, 0.5),
        loc='center left',
        borderaxespad=0.,
        frameon=False
    )

    # Ajustes finais
    plt.xticks(rotation=45, ha='right')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    # Salvar e mostrar resultados
    #os.makedirs("resultados_analise", exist_ok=True)
    #plt.savefig("resultados_analise/top10_paises_exportacao_evolucao.png", dpi=300, bbox_inches='tight')
    plt.show()

    spark.stop()