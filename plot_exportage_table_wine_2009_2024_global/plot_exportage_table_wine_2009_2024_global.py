import pandas as pd
import matplotlib.pyplot as plt

dfs = []

for year in range(2009, 2025):
    try:
        df_year = pd.read_csv(
            f'C:\\intellij-projects\\postech\\first-tech-challenge\\wine_exportage_data\\table_wine_exportage_data\\dados_{year}.csv',
            encoding="UTF-8", thousands=".", decimal=","
        )

        df_year["Valor (US$)"] = pd.to_numeric(df_year["Valor (US$)"], errors="coerce")
        df_year["Quantidade (Kg)"] = pd.to_numeric(df_year["Quantidade (Kg)"], errors="coerce")

        df_year = df_year.dropna(subset=["Valor (US$)", "Quantidade (Kg)"])

        df_year["Ano"] = year
        dfs.append(df_year)

    except FileNotFoundError:
        print(f"Dados de {year} não encontrados")

df_completo = pd.concat(dfs, ignore_index=True)

df_anual = df_completo.groupby('Ano').agg({'Valor (US$)': 'sum', 'Quantidade (Kg)': 'sum'}).reset_index()

df_anual['Valor (US$)'] = df_anual['Valor (US$)'].apply(lambda x: f'US$ {x:,.2f}')
df_anual['Quantidade (Kg)'] = df_anual['Quantidade (Kg)'].apply(lambda x: f'{x:,.0f} Kg')

print("\nTabela Anual Consolidada:")
print(df_anual.to_string(index=False))

plt.figure(figsize=(12, 6))

df_completo.groupby('Ano')[['Valor (US$)', 'Quantidade (Kg)']].sum().plot(
    kind='line',
    marker='o',
    secondary_y='Quantidade (Kg)',
    title='Exportação de Vinho por Ano'
)

plt.xlabel('Ano')
plt.ylabel('Valor (US$)')
#plt.right_ylabel('Quantidade (Kg)')
plt.grid(True)
plt.tight_layout()
plt.show()