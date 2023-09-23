-- OpenText Vertica-Magellan Telco Demo
-- SQL Scripts

-- DDL
create table tower (
    seq int, 
    tower_guid uuid,
    name varchar,
    city_seq int,
    city varchar,
    lat float,
    long float,
    size int,
    status varchar,
    avg_latency float
);

create table event (
    seq int, 
    event_ts timestamp,
    event_unix float,
    city int,
    tower int,
    device_guid uuid,
    account_guid uuid,
    device_type varchar,
    user_equipment varchar,
    slice_type varchar,
    request_ts timestamp,
    network_band varchar,
    packet_latency float,
    down_rate float,
    up_rate float,
    packet_loss float
);

-- Model Views
create view tower_for_kmeans as (
with t1 as (
select
  t.seq as "seq",
  lat,
  long,
  avg(packet_latency) as "avg_latency"
from tower t
left join event e
  on t.seq = e.tower
group by
  t.seq,
  lat,
  long
)
select * from t1
union all
select * from t1 where avg_latency > 0.1
union all
select * from t1 where avg_latency > 0.2
union all
select * from t1 where avg_latency > 0.3);

create view event_for_logistic_reg as (
select
  seq,
  packet_latency,
  down_rate,
  up_rate,
  packet_loss,
  case
    when packet_latency > 0.2 then 1
    else 0
  end as outcome
from event
);

-- Models
select kmeans('tower_kmeans', 'tower_for_kmeans', 'lat, long', 11 using parameters max_iterations=20, output_view='tower_clusters', key_columns='seq');
select logistic_reg('event_logistic_reg', 'event_for_logistic_reg', 'outcome', 'packet_latency, packet_loss');

-- K-Means Recommended Tower Locations
create view tower_recommended as (
    with centers as (
      select get_model_attribute ( using parameters model_name='tower_kmeans', attr_name='centers'))
    select
row_number() over () - 1 as seq,
'' as tower_guid,
'Recommended Tower #' || cast(row_number() over () as varchar) as name,
case 
  when floor(lat) between 48 and 52 then 1
  when floor(lat) between 34 and 38 then 2
  else 3
end as city_seq,
case 
  when floor(lat) between 48 and 52 then 'Frankfurt'
  when floor(lat) between 34 and 38 then 'Montreal'
  else 'Las Vegas'
end as city,
lat,
long,
3 as size,
'-1' as status,
0.0 as avg_latency
    from centers);


