select
    date,
    platform,
    campaign_name_raw,
    campaign_id,
    ad_group_name,
    taxonomy_id,
    spend,
    impressions,
    clicks,
    views,
    reach,
    conversions
from {{ source('trusted', 'campaigns_unified') }}
where platform = 'google_ads'
