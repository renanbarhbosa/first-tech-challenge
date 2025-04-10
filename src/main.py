import pandas as pd

# Carregar o arquivo CSV
table_wines_2024_raw = pd.read_csv(r'C:\intellij-projects\postech\first-tech-challenge\wine_data\table_wine\dados_2024.csv')

# Exibir informações iniciais
print("Head de exportação 2024")
print(table_wines_2024_raw.head())
print("Final do head de exportação 2024\n")

print("Início info")
table_wines_2024_raw.info()
print("Fim info\n")

table_wines_2024_raw["Valor (US$)"] = table_wines_2024_raw["Valor (US$)"].str.replace(r'[^\d.-]', '', regex=True)
table_wines_2024_raw["Valor (US$)"] = pd.to_numeric(table_wines_2024_raw["Valor (US$)"], errors='coerce')
table_wines_2024_raw.dropna(subset=["Valor (US$)"], inplace=True)
media_valor = table_wines_2024_raw["Valor (US$)"].mean()
print(f"Média do Valor (US$): {media_valor}")

print(table_wines_2024_raw)
