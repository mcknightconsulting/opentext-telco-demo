import sys
import math
import time
import datetime
import uuid
import random
import vertica_python

default_run_length = 120
default_loop_peak = 60
default_loop_count = 1

try:
    demo_run_length = int(sys.argv[1])
except:
    demo_run_length = default_run_length
try:
    demo_loop_peak = int(sys.argv[2])
except:
    demo_loop_peak = default_loop_peak
try:
    demo_loop_count = int(sys.argv[3])
except:
    demo_loop_count = default_loop_count

print("Demo will run for %d seconds"%(demo_run_length))
print("Degradation will peak at %d seconds"%(demo_loop_peak))
print("Demo will loop %d times"%(demo_loop_count))

device_types = [
    "Smartphone","Smartphone","Smartphone"
]
user_equipments = [
    "Samsung Galaxy S21+ 5G",
    "Samsung Galaxy S20 Ultra",
    "Samsung Galaxy S21 Ultra 5G",
    "Samsung Galaxy S21 5G",
    "Samsung Galaxy Note 10 Plus",
    "iPhone 8 Plus",
    "iPhone 8",
    "iPhone 12 Pro",
    "iPhone XS",
    "iPhone X",
]
slice_types = [
    "eMBB",
    "URLLC",
    "mMTC",
]
network_bands = [
    "LTE",
    "5G",
    "5G+",
]
poi = { 
    "Frankfurt": [
        ["Hauptwache",50.110924,8.682127],
        ["FRA",50.033333,8.570556],
        ["Deutsch Bank Park",50.068627,8.645486],
        ["Bornheim",50.129338,8.711038],
        ["Offenbach",50.104682,8.76154],
        ["Hauptfriedhof",50.138174,8.689555],
        ["Skyline Plaza",50.109372,8.652793],
        ["Eschborn",50.142322,8.56984],
        ["Kelsterbach",50.064588,8.530332]
    ],
    "Las Vegas": [
        ["Caesars Palace",36.1173,-115.1717],
        ["Downtown",36.188110,-115.176468],
        ["LAS",36.086010,-115.153969],
        ["Henderson",36.039524,-114.981720],
        ["Nellis AFB",36.241416,-115.050807],
        ["Spring Valley",36.113251,-115.308922,]
    ],
    "Montreal": [
        ["YUL",45.4697381,-73.74491],
        ["Laval",45.606649,-73.712409],
        ["Longueuil",45.536945,-73.510712],
        ["Boisbriand",45.613110,-73.838768],
        ["Anjou",45.6159585,-73.56935]
    ]
}
towers_per_poi = 10
city_towers = {
    1: [],
    2: [],
    3: []
}

# use 127.0.0.1 for local testing
conn_info = {'host': '127.0.0.1',
             'port': 5433,
             'user': 'dbadmin',
             'password': '',
             'database': 'VMart',
             'ssl': False,
             'autocommit': True,
             'use_prepared_statements': False,
             'connection_timeout': 5}

tries = 0
while tries < 60:
    try:
        conn = vertica_python.connect(**conn_info)
        if conn:
            break
    except:
        time.sleep(1)
        tries = tries + 1
        continue

if tries >= 60:
    print('Unable to connect to Vertica after %d seconds' % tries)
    exit(0)

cur = conn.cursor()
loop = 0

while loop < demo_loop_count:

    if loop > 0:
        cur.execute("select count(*) from event;")
        while True:
            rows = cur.fetchall()
            for row in rows:
                print("There are already %d events." % row[0])
            if not cur.nextset():
                break

        print(("%d Cities") % (len(poi)))
    else:
        cur.execute("drop view if exists tower_for_kmeans;")
        cur.execute("drop view if exists event_for_logistic_reg;")
        cur.execute("drop table if exists tower;")
        cur.execute("drop table if exists event;")

        cur.execute("""create table tower (
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
            );""")

        cur.execute("""create table event (
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
            );""")

        cur.execute("""create view tower_for_kmeans as (
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
            select * from t1 where avg_latency > 0.3)
            ;""")
        cur.execute("""create view event_for_logistic_reg as (
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
            )
            ;""")

    c = 1
    t = 1
    congestion_towers = []
    for city, pois in poi.items():
        p = 0
        congestion_poi = random.randrange(len(pois))
        for this_poi in pois:      
            for i in range(towers_per_poi):
                city_towers[c].append(t)
                if p == congestion_poi:
                    congestion_towers.append(t)
                if loop == 0:
                    data = (
                        t,
                        str(uuid.uuid4()),
                        this_poi[0] + " #" + str(t).zfill(4),
                        c,
                        city,
                        this_poi[1] + random.normalvariate(0, 0.058),
                        this_poi[2] + random.normalvariate(0, 0.058),
                        1,
                        1,
                        0.02
                    )
                    cur.execute("insert into tower values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", data)
                t = t + 1
            p = p + 1
        print(("%s Tower %d") % (city, t-1), end='')
        c = c + 1
        print()
        print('Congestion around %s' % (poi[city][congestion_poi][0]))

    i = 0
    starttime = time.time()
    timeout = starttime + demo_run_length
    delay = 0.0
    degradation = 0.0
    peak_congestion = demo_loop_peak
    congestion_mean = 1
    congestion_stddev = 0.333333
    while True:
        if time.time() > timeout:
            break
        i = i + 1
        this_timestamp = time.time()
        timeelapsed = time.time() - starttime
        x = timeelapsed / peak_congestion

        this_tower = random.randrange(1, t + 1)
        for this_city in range(1, 3 + 1):
            if this_tower in city_towers[this_city]:
                break

        this_network_band = random.choice(network_bands)
        this_packet_latency = 0
        this_packet_loss = 0
        while this_packet_latency <= 0 or this_packet_latency <= 0 :
            if this_network_band == 'LTE':
                this_packet_latency = random.normalvariate(0.02, 0.05)
            elif this_network_band == '5G':
                this_packet_latency = random.normalvariate(0.01, 0.02)
            else:
                this_packet_latency = random.normalvariate(0.005, 0.015)
            this_packet_loss = random.normalvariate(0.005, 0.005)

        if this_tower in congestion_towers:
            if timeelapsed > peak_congestion:            
                degradation = random.normalvariate(1, 0.3)
            else:
                degradation = ( 1 / ( congestion_stddev * math.sqrt( 2 * math.pi ) ) ) * math.exp( -0.5 * ( ( x - congestion_mean) / congestion_stddev ) ** 2 ) 
            this_packet_latency = this_packet_latency * 10 * degradation
            this_packet_loss = this_packet_loss * 20 * degradation
            delay = 0.0
        else:
            delay = 0.0
        
        data = (
            i, 
            datetime.datetime.fromtimestamp(this_timestamp).isoformat(sep=' '),
            this_timestamp,
            this_city,
            this_tower,
            str(uuid.uuid4()),
            str(uuid.uuid4()),
            random.choice(device_types),
            random.choice(user_equipments),
            random.choice(slice_types),
            (datetime.datetime.now() + datetime.timedelta(seconds=-3)).isoformat(sep=' '),
            this_network_band,
            this_packet_latency,
            0.6 / this_packet_latency,
            0.6 / this_packet_latency / random.normalvariate(5, 0.3),
            this_packet_loss
        )
        delay = 0.0
        while delay <= 0.0:
            delay = random.normalvariate(0.005, 0.005)
        time.sleep(delay)
        cur.execute("insert into event values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", data)

        if i % 100 == 0:
            print(("Event %d (x=%.3f Degradation=%.3f)                      \r") % (i, x, degradation), end='')

        if i % 2500 == 0 or i == 1:
            print(("Running k-means... Computing clusters...                \r"), end='')
            cur.execute("drop model if exists tower_kmeans;")
            cur.execute("drop view if exists tower_clusters;")
            cur.execute("select kmeans('tower_kmeans', 'tower_for_kmeans', 'lat, long', 11 using parameters max_iterations=20, output_view='tower_clusters', key_columns='seq');")
            cur.execute("drop view if exists tower_recommended;")
            cur.execute("""
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
            """)
        
        if i % 2500 == 0 or i == 100:
            print(("Running logisitic regression... Computing QoS scores...        \r"), end='')
            cur.execute("drop model if exists event_logistic_reg;")
            cur.execute("select logistic_reg('event_logistic_reg', 'event_for_logistic_reg', 'outcome', 'packet_latency, packet_loss');")

    print()
    loop = loop + 1

conn.close()
