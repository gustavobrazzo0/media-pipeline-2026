select
    platform,
    case platform
        when 'google_pmax'     then 'Google'
        when 'google_ads'      then 'Google'
        when 'google_shopping' then 'Google'
        when 'meta_ads'        then 'Meta'
        when 'dv360'           then 'Google'
    end as stack,
    case platform
        when 'google_pmax'     then 'Performance Max'
        when 'google_ads'      then 'Search / Video'
        when 'google_shopping' then 'Shopping'
        when 'meta_ads'        then 'Social'
        when 'dv360'           then 'Programmatic'
    end as platform_type,
    case platform
        when 'dv360' then false
        else true
    end as has_clicks
from (
    select distinct platform
    from {{ source('trusted', 'campaigns_unified') }}
)
