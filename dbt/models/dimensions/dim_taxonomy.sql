-- Surrogate keys canonicas das campanhas.
-- Fonte: dim_taxonomy_seed.csv, versionado no Git.
-- Cada taxonomy_id mapeia variacoes de nome de plataformas diferentes
-- para uma unica entidade de negocio com atributos estaveis.
select
    taxonomy_id,
    campaign_name_canonical,
    funnel_stage,
    product,
    objective,
    platform_primary
from {{ ref('dim_taxonomy_seed') }}
