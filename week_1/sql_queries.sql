SELECT count(1)
	FROM public.green_taxi_data
	where cast(lpep_pickup_datetime as date) = '2019-01-15'
	  and cast(lpep_dropoff_datetime as date) = '2019-01-15';

SELECT cast(lpep_pickup_datetime as date), max(trip_distance) as long_trip
	FROM public.green_taxi_data
	group by cast(lpep_pickup_datetime as date)
	order by 2 desc;

SELECT passenger_count, count(1) as cout_trip
	FROM public.green_taxi_data
	where cast(lpep_pickup_datetime as date) = '2019-01-01'
	  and passenger_count in (2,3)
	group by passenger_count
	order by 2 desc;

SELECT  zpu."Zone" as PUzone, zdo."Zone" as DOzone, max(tip_amount)
 FROM public.green_taxi_data as t
 inner join public.zone_lookup zpu
	     on  t."PULocationID" = zpu."LocationID"
 inner join public.zone_lookup zdo
	     on  t."DOLocationID" = zdo."LocationID"
 where zpu."Zone" = 'Astoria'
 group by 1, 2
 order by 3 desc;