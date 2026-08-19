-- Dimensao de campanha: une o nome canonico da taxonomy com os metadados
-- de plataforma e funil. Uma linha por campanha ativa no periodo.
select
    t.taxonomy_id,
    t.campaign_name_canonical,
    t.funnel_stage,
    t.product,
    t.objective,
    t.platform_primary,
    f.funnel_order,
    p.stack,
    p.platform_type
from {{ ref('dim_taxonomy') }} t
left join {{ ref('dim_funnel') }}   f on t.funnel_stage      = f.funnel_stage
left join {{ ref('dim_platform') }} p on t.platform_primary  = p.platform
