-- Pacing: realizado vs budget mensal por plataforma.
-- pacing = 1.0: na meta. pacing < 1.0: atrasado. pacing > 1.0: acelerado.
-- Fonte do budget: plano_de_midia do RAW (Excel do time de midia).
-- safe_cast porque Excel humano tem celulas com texto onde espera float.
with plano as (

    select 'google_pmax'     as platform, `Mes` as mes, safe_cast(`Google PMax`  as float64) as budget_mensal from {{ source('raw', 'plano_de_midia') }} where `Mes` is not null and safe_cast(`Google PMax`  as float64) is not null
    union all
    select 'google_ads',      `Mes`, safe_cast(`Google Ads`      as float64) from {{ source('raw', 'plano_de_midia') }} where `Mes` is not null and safe_cast(`Google Ads`      as float64) is not null
    union all
    select 'google_shopping', `Mes`, safe_cast(`Google Shopping` as float64) from {{ source('raw', 'plano_de_midia') }} where `Mes` is not null and safe_cast(`Google Shopping` as float64) is not null
    union all
    select 'meta_ads',        `Mes`, safe_cast(`Meta Ads`        as float64) from {{ source('raw', 'plano_de_midia') }} where `Mes` is not null and safe_cast(`Meta Ads`        as float64) is not null
    union all
    select 'dv360',           `Mes`, safe_cast(`DV360`           as float64) from {{ source('raw', 'plano_de_midia') }} where `Mes` is not null and safe_cast(`DV360`           as float64) is not null

),

realizado as (

    select
        platform,
        format_date('%b/%y', date) as mes,
        sum(spend)                 as spend_realizado
    from {{ ref('fct_campaign_daily') }}
    group by 1, 2

)

select
    p.mes,
    p.platform,
    p.budget_mensal,
    coalesce(r.spend_realizado, 0)                              as spend_realizado,
    safe_divide(coalesce(r.spend_realizado, 0), p.budget_mensal) as pacing
from plano p
left join realizado r
    on  p.platform = r.platform
    and p.mes      = r.mes
where p.budget_mensal is not null
  and p.budget_mensal > 0
order by p.mes, p.platform
