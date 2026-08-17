import os
import csv
import pandas as pd
import numpy as np
import random
from datetime import date, timedelta

'''
Gera CSVs simulados para 5 plataformas de mídia paga.
Nomes de campanha intencionalmente sujos para simular os problemas
que o pipeline resolve: typos, abreviações, capitalização inconsistente.
'''

random.seed(42)
np.random.seed(42)

INICIO = date(2026, 1, 1)
FIM    = date(2026, 12, 31)

CAMPANHAS = {
    "google_pmax": [
        {"nome": "pmax_awareness_cacaushow_2026",     "fase": "awareness",    "marca": "cacaushow"},
        {"nome": "pmax_awareness_kopenhagen_2026",    "fase": "awareness",    "marca": "kopenhagen"},
        {"nome": "pmx_awareness_harald_2026",         "fase": "awareness",    "marca": "harald"},
        {"nome": "pmax_remarketing_cacaushow_2026",   "fase": "conversao",    "marca": "cacaushow"},
    ],
    "google_ads": [
        {"nome": "search_brand_cacaushow_2026",       "fase": "consideracao", "marca": "cacaushow"},
        {"nome": "srch_brand_kopenhagen_2026",        "fase": "consideracao", "marca": "kopenhagen"},
        {"nome": "search_brand_Harald_2026",          "fase": "consideracao", "marca": "harald"},
        {"nome": "search_generic_chocolates_2026",    "fase": "conversao",    "marca": "cacaushow"},
        {"nome": "youtube_awareness_cacaushow_2026",  "fase": "awareness",    "marca": "cacaushow"},
    ],
    "google_shopping": [
        {"nome": "shopping_cacaushow_conversao_2026", "fase": "conversao",    "marca": "cacaushow"},
        {"nome": "shoping_kopenhagen_conversao_2026", "fase": "conversao",    "marca": "kopenhagen"},
        {"nome": "shopping_harald_conversao_2026",    "fase": "conversao",    "marca": "harald"},
    ],
    "meta_ads": [
        {"nome": "awareness_branding_cacaushow_2026",    "fase": "awareness",    "marca": "cacaushow"},
        {"nome": "awareness_branding_kopenhagen_2026",   "fase": "awareness",    "marca": "kopenhagen"},
        {"nome": "awareness_branding_Harald_2026",       "fase": "awareness",    "marca": "harald"},
        {"nome": "consideration_traffic_cacaushow_2026", "fase": "consideracao", "marca": "cacaushow"},
        {"nome": "cnversion_purchase_cacaushow_2026",    "fase": "conversao",    "marca": "cacaushow"},
        {"nome": "conversion_purchase_kopenhagen_2026",  "fase": "conversao",    "marca": "kopenhagen"},
    ],
    "dv360": [
        # insertion_order > line_item: hierarquia diferente de campaign > ad_group
        {"nome": "dv360_awareness_cacaushow_2026",    "fase": "awareness",    "marca": "cacaushow"},
        {"nome": "dv360_awareness_kopenhagen_2026",   "fase": "awareness",    "marca": "kopenhagen"},
        {"nome": "dv_awareness_Harald_2026",          "fase": "awareness",    "marca": "harald"},
        # operador entrou nome da campanha PMAX por engano no DV360
        # entity resolution vai mapear pmx_awareness_cacaushow_2026 -> id-2026000001 (mesmo que pmax)
        {"nome": "pmx_awareness_cacaushow_2026",      "fase": "awareness",    "marca": "cacaushow"},
    ],
}

TAXONOMY_IDS = {
    "pmax_awareness_cacaushow_2026":        "id-2026000001",
    "pmax_awareness_kopenhagen_2026":       "id-2026000002",
    "pmx_awareness_harald_2026":            "id-2026000003",
    "pmax_remarketing_cacaushow_2026":      "id-2026000004",
    "search_brand_cacaushow_2026":          "id-2026000005",
    "srch_brand_kopenhagen_2026":           "id-2026000006",
    "search_brand_Harald_2026":             "id-2026000007",
    "search_generic_chocolates_2026":       "id-2026000008",
    "youtube_awareness_cacaushow_2026":     "id-2026000009",
    "shopping_cacaushow_conversao_2026":    "id-2026000010",
    "shoping_kopenhagen_conversao_2026":    "id-2026000011",
    "shopping_harald_conversao_2026":       "id-2026000012",
    "awareness_branding_cacaushow_2026":    "id-2026000016",
    "awareness_branding_kopenhagen_2026":   "id-2026000017",
    "awareness_branding_Harald_2026":       "id-2026000018",
    "consideration_traffic_cacaushow_2026": "id-2026000019",
    "cnversion_purchase_cacaushow_2026":    "id-2026000020",
    "conversion_purchase_kopenhagen_2026":  "id-2026000021",
    "dv360_awareness_cacaushow_2026":       "id-2026000022",
    # pmx_awareness_cacaushow_2026 é o mesmo que pmax_awareness_cacaushow_2026
    # operador DV360 entrou o nome errado; entity resolution resolve aqui
    "pmx_awareness_cacaushow_2026":         "id-2026000001",
    "dv360_awareness_kopenhagen_2026":      "id-2026000023",
    "dv_awareness_Harald_2026":             "id-2026000024",
}


def dias_do_ano():
    d = INICIO
    while d <= FIM:
        yield d
        d += timedelta(days=1)


def distribuir_budget(total, n):
    # Dirichlet garante soma = 1. Fator simula sub/sobreentrega real de campanha.
    pesos = np.random.dirichlet(np.ones(n) * 2.5)
    return (pesos * total * random.uniform(0.85, 1.10)).tolist()


def impressoes(custo, cpm_base):
    cpm = cpm_base * random.uniform(0.8, 1.2)
    return max(1, int((custo / cpm) * 1000))


def cliques(imp, ctr_base):
    ctr = ctr_base * random.uniform(0.7, 1.3)
    return max(0, int(imp * ctr))


def gerar_google_pmax():
    datas  = list(dias_do_ano())
    linhas = []

    asset_groups = {
        "pmax_awareness_cacaushow_2026":   ["ag_pmax_topo_funil", "ag_pmax_remarketing"],
        "pmax_awareness_kopenhagen_2026":  ["ag_pmax_topo_funil"],
        "pmx_awareness_harald_2026":       ["ag_pmax_topo_funil"],
        "pmax_remarketing_cacaushow_2026": ["ag_pmax_remarketing"],
    }

    for c in CAMPANHAS["google_pmax"]:
        nome   = c["nome"]
        grupos = asset_groups.get(nome, ["ag_pmax_default"])
        cid    = random.randint(10000000000, 99999999999)
        budget = distribuir_budget(random.uniform(8000, 18000), len(datas))

        # IDs dos asset groups gerados uma vez por grupo, estaveis ao longo do tempo
        grupo_ids = {g: random.randint(1000000000, 9999999999) for g in grupos}

        for i, dia in enumerate(datas):
            for grupo in grupos:
                custo = round(budget[i] / len(grupos), 2)
                imp   = impressoes(custo, cpm_base=14)
                linhas.append({
                    "data":              dia.strftime("%Y-%m-%d"),
                    "nome_campanha":     nome,
                    "nome_grupo_assets": grupo,
                    "id_campanha":       cid,
                    "id_grupo_assets":   grupo_ids[grupo],
                    "custo":             custo,
                    "impressoes":        imp,
                    "cliques":           cliques(imp, 0.018),
                })

    return pd.DataFrame(linhas)


def gerar_google_ads():
    datas  = list(dias_do_ano())
    linhas = []

    # search: intenção alta -> CTR 5.8%, sem vídeo. youtube: branding -> CPM alto, views.
    config = {
        "search_brand_cacaushow_2026":     ("ag_search_brand",    "Chocolate premium - conheça",     "search"),
        "srch_brand_kopenhagen_2026":      ("ag_search_brand",    "Kopenhagen: presente perfeito",  "search"),
        "search_brand_Harald_2026":        ("ag_search_brand",    "Harald: qualidade profissional", "search"),
        "search_generic_chocolates_2026":  ("ag_search_genericos","Compare chocolates e economize",  "search"),
        "youtube_awareness_cacaushow_2026":("ag_youtube_topo",    "Descubra o sabor Cacau Show",     "youtube"),
    }

    for c in CAMPANHAS["google_ads"]:
        nome             = c["nome"]
        ag, headline, tp = config.get(nome, ("ag_default", "Anuncio", "search"))
        cid              = random.randint(10000000000, 99999999999)
        budget           = distribuir_budget(random.uniform(5000, 12000), len(datas))

        for i, dia in enumerate(datas):
            custo = round(budget[i], 2)

            if tp == "search":
                imp              = impressoes(custo, cpm_base=8)
                clq              = cliques(imp, 0.058)
                views_video      = 0
                views_100pct     = 0
                imp_mensuraveis  = int(imp * 0.96)
                imp_visiveis     = int(imp * 0.82)
            else:
                imp              = impressoes(custo, cpm_base=28)
                clq              = cliques(imp, 0.011)
                views_video      = int(imp * random.uniform(0.30, 0.45))
                views_100pct     = int(views_video * random.uniform(0.15, 0.25))
                imp_mensuraveis  = int(imp * 0.96)
                imp_visiveis     = int(imp * 0.75)

            linhas.append({
                "data":                   dia.strftime("%Y-%m-%d"),
                "nome_campanha":          nome,
                "nome_grupo_anuncio":     ag,
                "titulo":                 headline,
                "id_campanha":            cid,
                "termo_utm":              nome.replace("_2026", "").replace("_", "-"),
                "url_destino":            f"https://exemplo.com/lp/{nome.split('_')[0]}",
                "custo":                  custo,
                "impressoes":             imp,
                "cliques":                clq,
                "views_video":            views_video,
                "views_100pct":           views_100pct,
                "impressoes_mensuraveis": imp_mensuraveis,
                "impressoes_visiveis":    imp_visiveis,
            })

    return pd.DataFrame(linhas)


def gerar_google_shopping():
    datas  = list(dias_do_ano())
    linhas = []

    for c in CAMPANHAS["google_shopping"]:
        nome   = c["nome"]
        cid    = random.randint(10000000000, 99999999999)
        budget = distribuir_budget(random.uniform(4000, 10000), len(datas))

        for i, dia in enumerate(datas):
            custo = round(budget[i], 2)
            imp   = impressoes(custo, cpm_base=6)
            linhas.append({
                "data":          dia.strftime("%Y-%m-%d"),
                "nome_campanha": nome,
                "id_campanha":   cid,
                "custo":         custo,
                "impressoes":    imp,
                "cliques":       cliques(imp, 0.072),
            })

    return pd.DataFrame(linhas)


def gerar_meta_ads():
    datas  = list(dias_do_ano())
    linhas = []

    anuncios = {
        "awareness_branding_cacaushow_2026":    ("ad_video_topo_v1",    120215667889001),
        "awareness_branding_kopenhagen_2026":   ("ad_video_topo_v2",    120215667889002),
        "awareness_branding_Harald_2026":       ("ad_video_topo_v3",    120215667889003),
        "consideration_traffic_cacaushow_2026": ("ad_carousel_meio_v1", 120215667889004),
        "cnversion_purchase_cacaushow_2026":    ("ad_static_fundo_v1",  120215667889005),
        "conversion_purchase_kopenhagen_2026":  ("ad_static_fundo_v2",  120215667889006),
    }

    for c in CAMPANHAS["meta_ads"]:
        nome              = c["nome"]
        fase              = c["fase"]
        ad_nome, cid      = anuncios.get(nome, ("ad_default", 120215667889999))
        budget            = distribuir_budget(random.uniform(6000, 18000), len(datas))

        for i, dia in enumerate(datas):
            custo = round(budget[i], 2)

            if fase == "awareness":
                imp  = impressoes(custo, cpm_base=5.5)
                clq  = cliques(imp, 0.006)
                v3s  = int(imp * random.uniform(0.45, 0.60))
                v100 = int(v3s * random.uniform(0.15, 0.22))
                alc  = int(imp * random.uniform(0.55, 0.70))
            elif fase == "consideracao":
                imp  = impressoes(custo, cpm_base=9)
                clq  = cliques(imp, 0.018)
                v3s  = int(imp * random.uniform(0.30, 0.45))
                v100 = int(v3s * random.uniform(0.10, 0.18))
                alc  = int(imp * random.uniform(0.60, 0.75))
            else:
                imp  = impressoes(custo, cpm_base=16)
                clq  = cliques(imp, 0.034)
                v3s  = int(imp * random.uniform(0.15, 0.25))
                v100 = int(v3s * random.uniform(0.08, 0.15))
                alc  = int(imp * random.uniform(0.65, 0.80))

            linhas.append({
                "data":               dia.strftime("%Y-%m-%d"),
                "nome_campanha":      nome,
                "nome_anuncio":       ad_nome,
                "id_campanha":        cid,
                "link_post":          f"https://facebook.com/promo/{nome}",
                "custo":              custo,
                "impressoes":         imp,
                "cliques_link":       clq,
                "views_video_3s":     v3s,
                "views_video_100pct": v100,
                "alcance":            alc,
                "reacoes":            int(clq * random.uniform(1.8, 2.2)),
                "comentarios":        int(clq * random.uniform(0.10, 0.15)),
                "salvamentos":        int(clq * random.uniform(0.13, 0.18)),
                "compartilhamentos":  int(clq * random.uniform(0.15, 0.20)),
            })

    return pd.DataFrame(linhas)


def gerar_dv360():
    datas  = list(dias_do_ano())
    linhas = []

    # CPM alto reflete inventário premium (YouTube, GDN premium, CTV)
    line_items = [
        ("li_display_awareness_300x250", "creative_display_300x250_marca",  0.0),
        ("li_video_awareness_15s",       "creative_video_15s_storytelling", 0.82),
        ("li_video_awareness_30s",       "creative_video_30s_brand",        0.75),
    ]

    for c in CAMPANHAS["dv360"]:
        nome   = c["nome"]
        cid    = random.randint(10000000000, 99999999999)
        budget = distribuir_budget(random.uniform(10000, 22000), len(datas))

        for i, dia in enumerate(datas):
            for li, criativo, taxa_view in line_items:
                inv = round(budget[i] / len(line_items), 2)
                imp = impressoes(inv, cpm_base=38)
                linhas.append({
                    "data":            dia.strftime("%Y-%m-%d"),
                    "insertion_order": nome,
                    "line_item":       li,
                    "criativo":        criativo,
                    "id_campanha":     cid,
                    "investimento":    inv,
                    "impressoes":      imp,
                    "views_100pct":    int(imp * taxa_view * random.uniform(0.88, 1.05)),
                    "alcance":         int(imp * random.uniform(0.60, 0.75)),
                })

    return pd.DataFrame(linhas)


if __name__ == "__main__":
    plataformas = {
        "realizado_google_pmax":     gerar_google_pmax,
        "realizado_google_ads":      gerar_google_ads,
        "realizado_google_shopping": gerar_google_shopping,
        "realizado_meta_ads":        gerar_meta_ads,
        "realizado_dv360":           gerar_dv360,
    }

    for nome, fn in plataformas.items():
        df = fn()
        df.to_csv(f"data/{nome}.csv", index=False)
        print(f"{nome}: {len(df):,} linhas")

    # gera o arquivo de mapeamento para entity resolution no PySpark
    os.makedirs("config", exist_ok=True)
    with open("config/taxonomy_mapping.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["campaign_name_raw", "taxonomy_id"])
        for nome, tid in TAXONOMY_IDS.items():
            writer.writerow([nome, tid])
    print(f"taxonomy_mapping.csv: {len(TAXONOMY_IDS)} entradas")
