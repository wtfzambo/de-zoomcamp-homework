{% docs tripid %}This column represents a unique identifier for each trip.{% enddocs %}

{% docs vendorid %}This column represents the ID of the vendor that provided the service for the trip.{% enddocs %}

{% docs service_type %}This column represents the type of service provided for the trip (e.g. Green, Yellow).{% enddocs %}

{% docs ratecodeid %}This column represents the rate code for the trip.{% enddocs %}

{% docs pickup_locationid %}This column represents the ID of the location where the passenger was picked up.{% enddocs %}

{% docs pickup_borough %}This column represents the borough where the passenger was picked up.{% enddocs %}

{% docs pickup_zone %}This column represents the zone where the passenger was picked up.{% enddocs %}

{% docs dropoff_locationid %}This column represents the ID of the location where the passenger was dropped off.{% enddocs %}

{% docs dropoff_borough %}This column represents the borough where the passenger was dropped off.{% enddocs %}

{% docs dropoff_zone %}This column represents the zone where the passenger was dropped off.{% enddocs %}

{% docs pickup_datetime %}This column represents the date and time when the passenger was picked up.{% enddocs %}

{% docs dropoff_datetime %}This column represents the date and time when the passenger was dropped off.{% enddocs %}

{% docs store_and_fwd_flag %}This column indicates whether the trip data was held in vehicle memory before sending to the vendor because the vehicle did not have a connection to the server.{% enddocs %}

{% docs passenger_count %}This column represents the number of passengers on the trip.{% enddocs %}

{% docs trip_distance %}This column represents the distance of the trip in miles.{% enddocs %}

{% docs trip_type %}This column represents the type of the trip.{% enddocs %}

{% docs fare_amount %}This column represents the amount of the fare.{% enddocs %}

{% docs extra %}This column represents extra charges and fees.{% enddocs %}

{% docs mta_tax %}This column represents the tax collected from passengers by the Metropolitan Transportation Authority.{% enddocs %}

{% docs tip_amount %}This column represents the amount of tips given by passengers.{% enddocs %}

{% docs tolls_amount %}This column represents the total amount of all tolls paid in trip.{% enddocs %}

{% docs ehail_fee %}This column represents the total amount of the e-hail fee for the trip.{% enddocs %}

{% docs improvement_surcharge %}This column represents the improvement surcharge amount for the trip.{% enddocs %}

{% docs total_amount %}This column represents the total amount charged for the trip.{% enddocs %}

{% docs payment_type %}This column represents the payment type for the trip.{% enddocs %}

{% docs payment_type_description %}This column represents the description of the payment type for the trip.{% enddocs %}

{% docs congestion_surcharge %}This column represents the amount of the congestion surcharge for the trip.{% enddocs %}

{% docs revenue_zone %}
This column represents the pickup zone where the trip began and is used to group revenue by zone.
{% enddocs %}

{% docs revenue_month %}
This column represents the month in which the trip was made and is used to group revenue by month. The date is truncated to the beginning of the month using the date_trunc() function.
{% enddocs %}

{% docs service_type %}
This column represents the type of service provided during the trip (e.g. standard, premium, etc.).
{% enddocs %}

{% docs revenue_monthly_fare %}
This column represents the total fare revenue collected during the month, calculated by summing the fare_amount column for all trips that occurred during that month.
{% enddocs %}

{% docs revenue_monthly_extra %}
This column represents the total revenue collected from extra charges during the month, calculated by summing the extra column for all trips that occurred during that month.
{% enddocs %}

{% docs revenue_monthly_mta_tax %}
This column represents the total revenue collected from MTA tax during the month, calculated by summing the mta_tax column for all trips that occurred during that month.
{% enddocs %}

{% docs revenue_monthly_tip_amount %}
This column represents the total revenue collected from tips during the month, calculated by summing the tip_amount column for all trips that occurred during that month.
{% enddocs %}

{% docs revenue_monthly_tolls_amount %}
This column represents the total revenue collected from tolls during the month, calculated by summing the tolls_amount column for all trips that occurred during that month.
{% enddocs %}

{% docs revenue_monthly_ehail_fee %}
This column represents the total revenue collected from e-hail fees during the month, calculated by summing the ehail_fee column for all trips that occurred during that month.
{% enddocs %}

{% docs revenue_monthly_improvement_surcharge %}
This column represents the total revenue collected from improvement surcharges during the month, calculated by summing the improvement_surcharge column for all trips that occurred during that month.
{% enddocs %}

{% docs revenue_monthly_total_amount %}
This column represents the total revenue collected during the month, calculated by summing the total_amount column for all trips that occurred during that month.
{% enddocs %}

{% docs revenue_monthly_congestion_surcharge %}
This column represents the total revenue collected from congestion surcharges during the month, calculated by summing the congestion_surcharge column for all trips that occurred during that month.
{% enddocs %}

{% docs total_monthly_trips %}
This column represents the total number of trips made during the month, calculated by counting the number of tripid values for all trips that occurred during that month.
{% enddocs %}

{% docs avg_montly_passenger_count %}
This column represents the average number of passengers per trip during the month, calculated by averaging the passenger_count column for all trips that occurred during that month.
{% enddocs %}

{% docs avg_montly_trip_distance %}
This column represents the average trip distance during the month, calculated by averaging the trip_distance column for all trips that occurred during that month.
{% enddocs %}
