import os
import hashlib
import base64
import secrets
import datetime
import requests
import pandas as pd
import streamlit as st
from urllib.parse import urlencode
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

GCP_PROJECT   = os.getenv("GCP_PROJECT")
BQ_DATASET    = os.getenv("BQ_DATASET", "raw")
CLIENT_ID     = os.getenv("OAUTH_CLIENT_ID")
CLIENT_SECRET = os.getenv("OAUTH_CLIENT_SECRET")
REDIRECT_URI  = os.getenv("OAUTH_REDIRECT_URI", "http://localhost:8501")

# realizado: gerar_dados.py -> data/*.csv | plano: limpar_plano.py -> data/plano_*.csv
TABELAS = {
    "realizado_google_pmax.csv":     f"{BQ_DATASET}.realizado_google_pmax",
    "realizado_google_ads.csv":      f"{BQ_DATASET}.realizado_google_ads",
    "realizado_google_shopping.csv": f"{BQ_DATASET}.realizado_google_shopping",
    "realizado_meta_ads.csv":        f"{BQ_DATASET}.realizado_meta_ads",
    "realizado_dv360.csv":           f"{BQ_DATASET}.realizado_dv360",
    "plano_de_midia.csv":            f"{BQ_DATASET}.plano_de_midia",
    "plano_google_pmax.csv":         f"{BQ_DATASET}.plano_google_pmax",
    "plano_google_ads.csv":          f"{BQ_DATASET}.plano_google_ads",
    "plano_meta_ads.csv":            f"{BQ_DATASET}.plano_meta_ads",
}


def gerar_pkce():
    verifier  = base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode()
    digest    = hashlib.sha256(verifier.encode()).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
    return verifier, challenge


def url_login(challenge, verifier):
    params = {
        "client_id":             CLIENT_ID,
        "redirect_uri":          REDIRECT_URI,
        "response_type":         "code",
        "scope":                 "openid email profile",
        "code_challenge":        challenge,
        "code_challenge_method": "S256",
        "state":                 verifier,  # verifier viaja no state: Streamlit e stateless
        "access_type":           "offline",
        "prompt":                "consent",
    }
    return f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(params)}"


def trocar_code(code, verifier):
    resp = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "code":          code,
            "client_id":     CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "redirect_uri":  REDIRECT_URI,
            "grant_type":    "authorization_code",
            "code_verifier": verifier,
        }
    )
    resp.raise_for_status()
    return resp.json()


def info_usuario(token):
    resp = requests.get(
        "https://www.googleapis.com/oauth2/v1/userinfo",
        headers={"Authorization": f"Bearer {token}"}
    )
    resp.raise_for_status()
    return resp.json()


def fazer_login():
    params = st.query_params

    if "code" in params and "state" in params and "usuario" not in st.session_state:
        try:
            token_data   = trocar_code(params["code"], params["state"])
            access_token = token_data.get("access_token")
            if not access_token:
                st.error(f"falha ao obter token: {token_data}")
                return False
            st.session_state["usuario"] = info_usuario(access_token)
            st.query_params.clear()
            st.rerun()
        except Exception as e:
            st.error(f"erro na autenticacao: {e}")
            return False

    if "usuario" not in st.session_state:
        st.title("Media Pipeline: Ingestion RAW")
        st.markdown("---")
        st.markdown("### faca login para continuar")
        verifier, challenge = gerar_pkce()
        st.link_button("entrar com Google", url_login(challenge, verifier), use_container_width=True)
        return False

    return True


def limpar_df(df):
    '''Converte NaN -> None antes de enviar ao BigQuery.
    PyArrow rejeita float NaN em colunas de tipo object.
    RAW aceita tudo como string; tipos sao corrigidos no PySpark.'''
    return df.astype(object).where(pd.notna(df), other=None)


def enviar(client, df, tabela, modo):
    df  = limpar_df(df)
    cfg = bigquery.LoadJobConfig(write_disposition=modo, autodetect=True)
    try:
        client.load_table_from_dataframe(df, tabela, job_config=cfg).result()
        return True, f"ok: `{tabela}` - {len(df):,} linhas"
    except Exception as e:
        return False, f"erro: `{tabela}` - {e}"


def salvar_log(usuario, resultados, modo):
    agora    = datetime.datetime.now()
    filename = f"log_ingestao_{agora.strftime('%Y-%m-%d_%H-%M')}.txt"
    corpo    = "\n".join([
        "=" * 60,
        "LOG DE INGESTAO - MEDIA PIPELINE 2026",
        "=" * 60,
        f"data/hora  : {agora.strftime('%Y-%m-%d %H:%M:%S')}",
        f"usuario    : {usuario.get('name', '?')} ({usuario.get('email', '?')})",
        f"projeto BQ : {GCP_PROJECT}",
        f"dataset    : {BQ_DATASET}",
        f"modo       : {modo}",
        "",
        "RESULTADOS:",
        "-" * 40,
        *resultados,
        "",
        "=" * 60,
    ])
    with open(filename, "w", encoding="utf-8") as f:
        f.write(corpo)
    return filename, corpo


def main():
    st.set_page_config(
        page_title="Media Pipeline: Ingestao RAW",
        page_icon=None,
        layout="centered"
    )

    if not fazer_login():
        return

    usuario = st.session_state["usuario"]

    with st.sidebar:
        if usuario.get("picture"):
            st.image(usuario["picture"], width=56)
        st.markdown(f"**{usuario.get('name', '')}**")
        st.caption(usuario.get("email", ""))
        st.markdown("---")
        st.caption("camada de destino")
        st.code(f"{GCP_PROJECT}\n  {BQ_DATASET} (RAW)")
        st.markdown("---")
        if st.button("sair", use_container_width=True):
            st.session_state.clear()
            st.rerun()

    st.title("Media Pipeline: Ingestao RAW")
    st.markdown(
        "Envie os arquivos de midia para a camada **RAW** do BigQuery. "
        "Nenhuma transformacao e aplicada: o dado chega como esta na origem."
    )
    st.markdown("---")

    st.subheader("1 - selecione os arquivos")
    arquivos = st.file_uploader(
        "arraste ou clique para selecionar",
        accept_multiple_files=True,
        type=["csv"],
        help="CSVs do realizado + CSVs do plano (gerados por limpar_plano.py)"
    )

    if not arquivos:
        st.info("nenhum arquivo selecionado.")
        return

    st.subheader("2 - confira os arquivos")
    csvs = {}

    for arquivo in arquivos:
        nome = arquivo.name
        col1, col2 = st.columns([3, 1])
        try:
            df = pd.read_csv(arquivo)
            with col1:
                st.markdown(f"**{nome}**")
            with col2:
                st.caption(f"{len(df):,} linhas · {len(df.columns)} cols")
            st.dataframe(df.head(5), use_container_width=True)
            if nome not in TABELAS:
                st.warning(f"`{nome}` nao mapeado: sera ignorado.")
            else:
                csvs[nome] = df
        except Exception as e:
            st.error(f"erro ao ler `{nome}`: {e}")
        st.markdown("---")

    if not csvs:
        st.warning("nenhum arquivo valido para enviar.")
        return

    st.subheader("3 - configure o envio")
    modo = st.radio(
        "modo de escrita:",
        options=["WRITE_TRUNCATE", "WRITE_APPEND"],
        captions=[
            "apaga e substitui (use na primeira carga)",
            "adiciona sem apagar (use para incrementar)",
        ],
        horizontal=True,
    )
    st.markdown("---")

    st.subheader("4 - enviar")
    if st.button("enviar para o BigQuery", use_container_width=True, type="primary"):
        client     = bigquery.Client(project=GCP_PROJECT)
        resultados = []
        progress   = st.progress(0, text="iniciando...")

        for i, (nome, df) in enumerate(csvs.items()):
            tabela = f"{GCP_PROJECT}.{TABELAS[nome]}"
            progress.progress(i / len(csvs), text=f"enviando {nome}...")
            ok, msg = enviar(client, df, tabela, modo)
            resultados.append(msg)
            st.success(msg) if ok else st.error(msg)

        progress.progress(1.0, text="concluido!")
        filename, corpo = salvar_log(usuario, resultados, modo)
        st.markdown("---")
        st.subheader("log da sessao")
        st.code(corpo)
        st.caption(f"salvo em: `{filename}`")


if __name__ == "__main__":
    main()
