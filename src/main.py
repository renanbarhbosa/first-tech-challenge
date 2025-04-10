#TODO - AJUSTAR A ROTAÇÃO COM OS PAÍSES E EVITAR A SOBREPOSIÇÃO

import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv(r'C:\intellij-projects\postech\first-tech-challenge\wine_data\table_wine\dados_2024.csv',
                 encoding="UTF-8", thousands=".", decimal=",")

df["Valor (US$)"] = pd.to_numeric(df["Valor (US$)"], errors="coerce")
df = df.dropna(subset=["Valor (US$)"])

media = df["Valor (US$)"].mean()
print(f"\nMédia do valor em (US$) é: {media:.2f}\n")

df = df.sort_values(by="Valor (US$)", ascending=False)
print(df)

ax = df.plot(x="Países", y="Valor (US$)", figsize=(10, 6), kind="bar")
plt.ylim(0, 1000)
plt.xticks(rotation=45)
ax.tick_params(axis='x', labelsize=10, pad=10)
ax.set_title("Valor de Exportação de Vinho por País (US$)")
ax.set_xlabel("Países")
ax.set_ylabel("Valor (US$)")
plt.tight_layout()
plt.show()
