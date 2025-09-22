select
    hour(pickup_datetime) as `hour`,
    round(avg(passenger_count), 2) as average_count
from
    trusted.taxi_data_unified
where
    year(pickup_datetime) = 2023
    and month(pickup_datetime) = 5
group by
    hour(pickup_datetime)
order by
    hour(pickup_datetime)
