import pandas as pd
import matplotlib.pyplot as plt

# Carregar os dados da planilha "Resultado"
df = pd.read_excel("C:\\intellij-projects\\postech\\first-tech-challenge\\precos-exportacao-via-rodoviaria\\argentina\\file\\V_EXPORTACAO_GERAL_2009-01_2024-12_DT20250507.xlsx")

# Filtrar dados relevantes
filtro = (
    (df["Países"] == "Argentina") &
    (df["UF do Produto"] == "Rio Grande do Sul") &
    (df["Código CUCI Grupo"].isin([48, 57, 58, 59]))) # Códigos CUCI como números

df_filtrado = df[filtro]

# Calcular eficiência (Valor FOB por kg)
df_filtrado["Eficiência (US$/kg)"] = df_filtrado["Valor US$ FOB"] / df_filtrado["Quilograma Líquido"]

# Gráfico de dispersão
plt.figure(figsize=(10, 6))
cores = {"RODOVIARIA": "blue", "AEREA": "red"}
plt.scatter(
    df_filtrado["Quilograma Líquido"],
    df_filtrado["Valor US$ FOB"],
    c=df_filtrado["Via"].map(cores),
    alpha=0.6,
    label=df_filtrado["Via"]
)

plt.xlabel("Quilograma Líquido")
plt.ylabel("Valor FOB (US$)")
plt.title("Relação entre Valor FOB e Peso (Argentina - RS)")
plt.legend(title="Via", labels=["RODOVIARIA", "AEREA"])
plt.grid(True)
plt.show()

# Comparar eficiência média por via
eficiencia_por_via = df_filtrado.groupby("Via")["Eficiência (US$/kg)"].mean()
print("\nEficiência Média por Via:")
print(eficiencia_por_via.to_string())