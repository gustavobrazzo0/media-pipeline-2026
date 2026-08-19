-- Fonte unica para o filtro de data do Looker Studio.
-- Reune metricas de volume (mart_cross_channel) com benchmarks (mart_kpi_summary)
-- num unico dataset para que um filtro de data controle todos os graficos.
select
    c.date,
    c.platform,
    c.stack,
    c.platform_type,
    c.taxonomy_id,
    c.campaign_name_canonical,
    c.funnel_stage,
    c.funnel_order,
    c.product,
    c.objective,
    c.spend,
    c.impressions,
    c.clicks,
    c.views,
    c.reach,
    c.conversions,
    c.ctr,
    c.cpc,
    c.cpm,
    c.vtr,
    c.cpa,
    k.ctr_benchmark_variance,
    k.cpm_benchmark_variance,
    k.vtr_benchmark_variance
from {{ ref('mart_cross_channel') }} c
left join {{ ref('mart_kpi_summary') }} k
    using (taxonomy_id, platform)
