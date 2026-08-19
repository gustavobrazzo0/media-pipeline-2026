-- Grain: uma linha por mes x plataforma.
-- Agrega fct_campaign_daily para metricas mensais por canal.
-- Usada por mart_kpi_summary para calculos de benchmark cross-canal.
select
    date_trunc(date, month)                              as mes,
    platform,
    stack,
    sum(spend)                                           as spend_total,
    sum(impressions)                                     as impressions_total,
    sum(clicks)                                          as clicks_total,
    sum(views)                                           as views_total,
    sum(conversions)                                     as conversions_total,
    safe_divide(sum(clicks),      sum(impressions))      as ctr_medio,
    safe_divide(sum(spend),       sum(clicks))           as cpc_medio,
    safe_divide(sum(spend),       sum(impressions)) * 1000 as cpm_medio,
    safe_divide(sum(views),       sum(impressions))      as vtr_medio,
    safe_divide(sum(spend),       sum(conversions))      as cpa_medio
from {{ ref('fct_campaign_daily') }}
group by 1, 2, 3
