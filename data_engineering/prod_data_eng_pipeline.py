import pandas as pd
import numpy as np

# Example Data Pre-processing
df = pd.read_csv("assets/MiBolsillo.csv", sep=";", encoding="latin1")
df.rename(columns={" valor ": "valor"}, inplace=True)

df["valor"] = df["valor"].str.replace(".", "")
df["valor"] = df["valor"].str.replace(",", ".")
df["valor"] = df["valor"].str.replace("-", "")
df["valor"] = df["valor"].str.replace(" ", "")
df["valor"].fillna(np.nan, inplace=True)
df["valor"] = pd.to_numeric(df["valor"])

df["is_vital"] = df["grupo_estabelecimento"].apply(
    lambda x: 1
    if x in ["SERVIO", "SUPERMERCADOS", "FARMACIAS", "HOSP E CLINICA", "TRANS FINANC"]
    else 0
)

df["data"] = pd.to_datetime(df["data"], format="%d.%m.%Y")

df["limit_disp_percentage"] = (df["limite_disp"] * 100) / df["limite_total"]
df["limit_disp_percentage"] = df["limit_disp_percentage"].round().astype(int)

df["is_revenge_spending"] = df.apply(
    lambda row: 1 if row["limit_disp_percentage"] < 20 and row["is_vital"] == 0 else 0,
    axis=1,
)

# Save the master table
df.to_csv("master_table.csv", index=False, sep=";", encoding="latin1")
