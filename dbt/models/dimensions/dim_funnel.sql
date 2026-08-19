-- Ordem do funil define a sequencia awareness -> consideracao -> conversao.
-- funnel_order e usado para ordenacao em dashboards e para calculos de
-- taxa de avanco entre etapas.
select
    funnel_stage,
    case funnel_stage
        when 'awareness'    then 1
        when 'consideracao' then 2
        when 'conversao'    then 3
    end as funnel_order,
    case funnel_stage
        when 'awareness'    then 'Topo de funil: alcance e reconhecimento de marca'
        when 'consideracao' then 'Meio de funil: intencao e consideracao de compra'
        when 'conversao'    then 'Fundo de funil: compra e retorno sobre investimento'
    end as funnel_description
from (
    select distinct funnel_stage
    from {{ ref('dim_taxonomy') }}
)
