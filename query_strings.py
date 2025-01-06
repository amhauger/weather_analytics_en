"""
HELPERS
"""

begin = """
begin;
"""

commit = """
commit;
"""

rollback = """
rollback;
"""

"""
GETTERS
"""
get_orders_by_store_number = """
select 
    date_trunc('hour', convert_timezone('UTC', dl.timezone_id, o.created_at)) as date_time,
    count(distinct o.order_id) as car_count 
from 
    dw.orders o
inner join 
    dw.dim_locations dl on o.location_id = dl.location_id
where 
    o.created_at >= '{}'::date
and
    o.created_at < '{}'::date
and
    o.deleted_at is null
and
    o.is_business_hours = 'true'
and
    o.service_item_id is not null
and
    dl.location_number = '{}'
group by 1
order by 1
"""
get_stores_by_zipcode = """
select 
    loc.region_number,
    wsp.location_number,
    wsp.is_closed_sunday,
    wsp.summer_hours_open,
    wsp.summer_hours_close,
    wsp.winter_hours_open,
    wsp.winter_hours_close,
    wsp.time_zone
from 
    public.weather_iq_store_parameters as wsp
join
    dw.dim_locations as loc
on
    wsp.location_number = loc.location_number
where
    loc.zip_code = {}
"""

get_historic_weather_for_zip_code = """
select 
    weather_date,
    condition_text,
    total_precipitation
from 
    dw.weather 
where 
    zip_code = {} 
and 
    condition_text is not NULL
order by
    weather_date asc
"""

get_distinct_zip_codes = """
select distinct
    zip_code
from
    dw.dim_locations
"""

get_weather_for_zip = """
select 
    weather_date, 
    condition_text, 
    total_precipitation 
from 
    dw.weather 
where 
    zip_code = {} and condition_text is not NULL 
order by 
    weather_date asc
"""

get_store_params = """
select 
    is_closed_sunday, 
    summer_hours_open, 
    summer_hours_close, 
    winter_hours_open, 
    winter_hours_close,
    time_zone,
    l1_ratio,
    alpha,
    zip_code
from 
    public.weather_iq_store_parameters 
where 
    location_number = {}
"""

get_car_count_for_store = """
select 
    date_trunc('hour', convert_timezone('UTC', dl.timezone_id, o.created_at)) as date_time,
    count(distinct o.order_id) as car_count 
from 
    dw.orders o
inner join 
    dw.dim_locations dl on o.location_id = dl.location_id
where 
    o.created_at >= '{}'::date
and
    o.created_at < '{}'::date
and
    o.deleted_at is null
and
    o.is_business_hours = 'true'
and
    o.service_item_id is not null
and
    dl.location_number = '{}'
group by 1
order by 1
 """

'''
SETTERS
'''
update_store_en_fields = """
update
    public.weather_iq_store_parameters
set
    l1_ratio = {},
    alpha = {},
    en_mae = {},
    updated_at = getdate()
where
    location_number = '{}';
"""

insert_region_coefficient_fields = """
insert into
    public.weather_analytics_region_coefficients
values
    region_number = {},
    hour_coefficient = {},
    precipitation_coefficient = {},
    is_holiday_coefficient = {},
    adj_hours_coefficeint = {},
    cloud_cover_coefficient = {},
    rain_intensity_coefficient = {},
    sleet_intensity_coefficient = {},
    snow_intensity_coefficient = {},
    ice_intensity_coefficient = {},
    thunder_intensity_coefficient = {},
    created_at = getdate(),
    updated_at = getdate()
"""