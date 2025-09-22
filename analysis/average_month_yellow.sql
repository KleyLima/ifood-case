select
    month(pickup_datetime) as month,
    round(avg(total_amount), 2) as average_amount
from
    trusted.taxi_data_unified
where
    cab_type = 'yellow'
group by
    month(pickup_datetime)
order by
    month(pickup_datetime)
