-- Visao cross-canal: performance de cada campanha em cada plataforma.
-- Consumido pelo Looker Studio para a aba de Performance.
-- Grain: dia x plataforma x taxonomy_id.
select
    date,
    platform,
    stack,
    platform_type,
    taxonomy_id,
    campaign_name_canonical,
    funnel_stage,
    funnel_order,
    product,
    objective,
    sum(spend)       as spend,
    sum(impressions) as impressions,
    sum(clicks)      as clicks,
    sum(views)       as views,
    sum(reach)       as reach,
    sum(conversions) as conversions,
    safe_divide(sum(clicks),      sum(impressions))       as ctr,
    safe_divide(sum(spend),       sum(clicks))            as cpc,
    safe_divide(sum(spend),       sum(impressions)) * 1000 as cpm,
    safe_divide(sum(views),       sum(impressions))       as vtr,
    safe_divide(sum(spend),       sum(conversions))       as cpa
from {{ ref('fct_campaign_daily') }}
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
