-- KPIs de cada campanha comparados com a media do canal (benchmark).
-- benchmark_variance > 0: campanha acima da media do canal.
-- benchmark_variance < 0: campanha abaixo da media do canal.
with medias_canal as (

    select
        platform,
        avg(ctr) as avg_ctr_platform,
        avg(cpm) as avg_cpm_platform,
        avg(vtr) as avg_vtr_platform,
        avg(cpc) as avg_cpc_platform
    from {{ ref('fct_platform_performance') }}
    group by platform

),

campanhas as (

    select
        taxonomy_id,
        campaign_name_canonical,
        platform,
        funnel_stage,
        product,
        sum(spend)                                            as spend_total,
        safe_divide(sum(clicks),      sum(impressions))       as ctr,
        safe_divide(sum(spend),       sum(impressions)) * 1000 as cpm,
        safe_divide(sum(views),       sum(impressions))       as vtr,
        safe_divide(sum(spend),       sum(clicks))            as cpc,
        safe_divide(sum(spend),       sum(conversions))       as cpa
    from {{ ref('fct_campaign_daily') }}
    group by 1, 2, 3, 4, 5

)

select
    c.*,
    m.avg_ctr_platform,
    m.avg_cpm_platform,
    m.avg_vtr_platform,
    m.avg_cpc_platform,
    safe_divide(c.ctr, m.avg_ctr_platform) - 1 as ctr_benchmark_variance,
    safe_divide(c.cpm, m.avg_cpm_platform) - 1 as cpm_benchmark_variance,
    safe_divide(c.vtr, m.avg_vtr_platform) - 1 as vtr_benchmark_variance
from campanhas c
left join medias_canal m on c.platform = m.platform
