import pandas as pd
import os

ARQUIVO    = "data/plano_de_midia_2026.xlsx"
PASTA      = "data"

ABAS = {
    "plano_de_midia": "plano_de_midia.csv",
    "google_pmax":    "plano_google_pmax.csv",
    "google_ads":     "plano_google_ads.csv",
    "meta_ads":       "plano_meta_ads.csv",
}


def detectar_header(df_raw):
    '''Planilhas humanas têm linhas de título antes do cabeçalho real.
    Retorna o índice da primeira linha com 3+ colunas preenchidas.'''
    for i, row in df_raw.iterrows():
        if row.notna().sum() >= 3:
            return i
    return None


xl = pd.ExcelFile(ARQUIVO)

for aba, nome_csv in ABAS.items():
    if aba not in xl.sheet_names:
        print(f"aba '{aba}' não encontrada, pulando")
        continue

    df_raw     = pd.read_excel(ARQUIVO, sheet_name=aba, header=None, engine="openpyxl")
    header_row = detectar_header(df_raw)

    if header_row is None:
        print(f"aba '{aba}': cabeçalho não encontrado")
        continue

    df = (
        pd.read_excel(ARQUIVO, sheet_name=aba, header=header_row, engine="openpyxl")
        .dropna(how="all")
        .loc[:, lambda x: ~x.columns.str.startswith("Unnamed")]
        .astype(str)
        .replace("nan", "")
    )

    caminho = os.path.join(PASTA, nome_csv)
    df.to_csv(caminho, index=False, encoding="utf-8")
    print(f"{aba} salvo em {caminho} ({len(df)} linhas, {len(df.columns)} colunas)")
    print(f"  colunas: {list(df.columns)}")
