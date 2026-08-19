-- Grain: uma linha por dia x plataforma x campanha x ad_group.
-- KPIs calculados aqui sao a fonte de verdade para todos os marts.
-- nullif(x, 0) evita divisao por zero retornando null: metrica indefinida,
-- nao erro. Metrica indefinida e semanticamente diferente de zero.
with all_platforms as (

    select * from {{ ref('stg_google_pmax') }}
    union all
    select * from {{ ref('stg_google_ads') }}
    union all
    select * from {{ ref('stg_google_shopping') }}
    union all
    select * from {{ ref('stg_meta_ads') }}
    union all
    select * from {{ ref('stg_dv360') }}

),

com_dimensoes as (

    select
        a.date,
        a.platform,
        a.campaign_name_raw,
        a.campaign_id,
        a.ad_group_name,
        a.taxonomy_id,
        a.spend,
        a.impressions,
        a.clicks,
        a.views,
        a.reach,
        a.conversions,
        t.campaign_name_canonical,
        t.funnel_stage,
        t.product,
        t.objective,
        p.stack,
        p.platform_type,
        p.has_clicks,
        f.funnel_order
    from all_platforms a
    left join {{ ref('dim_taxonomy') }}  t on a.taxonomy_id = t.taxonomy_id
    left join {{ ref('dim_platform') }}  p on a.platform    = p.platform
    left join {{ ref('dim_funnel') }}    f on t.funnel_stage = f.funnel_stage

),

com_kpis as (

    select
        *,
        safe_divide(clicks,      impressions)       as ctr,
        safe_divide(spend,       clicks)            as cpc,
        safe_divide(spend,       impressions) * 1000 as cpm,
        safe_divide(views,       impressions)       as vtr,
        safe_divide(spend,       conversions)       as cpa
    from com_dimensoes

)

select * from com_kpis
