import pandas as pd
import matplotlib.pyplot as plt

# Configurações iniciais
FILE_PATH = "C:\\intellij-projects\\postech\\first-tech-challenge\\precos-exportacao-via-rodoviaria\\espanha\\file\\V_EXPORTACAO_GERAL_2009-01_2024-12_DT20250509.xlsx"
CUCI_RELEVANTES = [48, 57, 58, 59]  # Códigos numéricos conforme dataset


# Carregar e filtrar dados
def carregar_dados():
    df = pd.read_excel(FILE_PATH, sheet_name="Resultado")

    return df[
        (df["Países"] == "Espanha") &
        (df["UF do Produto"] == "Rio Grande do Sul") &
        (df["Código CUCI Grupo"].isin(CUCI_RELEVANTES))
        ]


# Análise de eficiência
def analisar_eficiencia(df):
    df["Eficiência (US$/kg)"] = df["Valor US$ FOB"] / df["Quilograma Líquido"]

    # Gráfico
    plt.figure(figsize=(12, 7))
    cores = {"MARITIMA": "navy", "AEREA": "crimson"}

    plt.scatter(
        x=df["Quilograma Líquido"],
        y=df["Valor US$ FOB"],
        c=df["Via"].map(cores),
        alpha=0.7,
        edgecolors='w',
        s=100
    )

    plt.xlabel("Peso Líquido (kg)", fontsize=12)
    plt.ylabel("Valor FOB (US$)", fontsize=12)
    plt.title("Relação Valor vs Peso nas Exportações para Espanha - RS (2009-2024)", fontsize=14, pad=20)
    plt.grid(True, linestyle='--', alpha=0.7)

    # Legenda personalizada
    handles = [
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=cores["MARITIMA"], markersize=10, label='Marítima'),
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=cores["AEREA"], markersize=10, label='Aérea')
    ]
    plt.legend(handles=handles, title="Via de Transporte", fontsize=10)

    plt.tight_layout()
    plt.show()

    # Estatísticas
    eficiencia = df.groupby("Via", as_index=False)["Eficiência (US$/kg)"].mean()
    print("\n🔍 Eficiência Média por Via de Transporte:")
    print(eficiencia.to_string(index=False, float_format="US$ %.2f"))


# Execução principal
if __name__ == "__main__":
    dados = carregar_dados()
    analisar_eficiencia(dados)