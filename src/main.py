import pandas as pd
import matplotlib.pyplot as plt  # Corrigido
import seaborn as sbn
import numpy as np

df = pd.read_csv(r'C:\intellij-projects\postech\first-tech-challenge\wine_data\table_wine\dados_2024.csv',
                 encoding="UTF-8", thousands=".", decimal=",")

df["Valor (US$)"] = pd.to_numeric(df["Valor (US$)"], errors="coerce")

df = df.dropna(subset=["Valor (US$)"])

media = df["Valor (US$)"].mean(skipna=True)

pd.options.display.float_format = "{:.2f}".format

print(f"Média do valor em (US$) é: {media}\n")
print(df)

df.plot(x="Países", y="Valor (US$)", figsize=(12,12), kind="bar")
plt.show()  # Exibir gráfico corretamente
