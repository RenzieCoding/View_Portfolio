------------------------------------------------------------------------------------------------
-- Ride Hailing App Project
-- by Renzie Reyes
-- Source: https://www.kaggle.com/datasets/galihwardiana/ride-hailing-transaction
-- This project focuses on the analysis of ride-hailing transaction data, including user details,
-- driver information, and ride statistics.
------------------------------------------------------------------------------------------------
```sql 
--SECTION: DATABASE SET-UP

CREATE DATABASE ride_hailing_analysis;

--SECTION: CREATING TABLES

--drivers table
CREATE TABLE drivers (
	driver_id bigint PRIMARY KEY,														--driver ID and must be unique
	first_name text,																	--driver's first name
	last_name text,																		--driver's last name
	email text,																			--driver's email
	phone_number text ,																	--driver's phone number (if possible, each driver must have one)
	car_make text,																		--car make(text)
	car_model text,																		--car model(text)	
	license_plate  text,																--licese plate(text followed by a dash and 4 numbers)				
	created_at date																		--date of data entry
);

--users Table
CREATE TABLE users ( 
	user_id bigint PRIMARY KEY,
	username text,
	last_name text,
	first_name text,
	email text,
	password_hash text,
	phone_number text,
	gender text,
	created_at date
);
--vouchers Table


CREATE TABLE vouchers (
	voucher_code text,
	discount_amount numeric (10,2),
	start_date bigint,
	end_date bigint,
	updated_at bigint,
	discount_perc numeric (5,0)
);
--fact_rides

SELECT * FROM fact_rides
CREATE TABLE fact_rides (
	ride_id bigint,
	customer_id bigint,
	driver_id bigint,
	start_location text,
	end_location text,
	city text,
	country text,
	ride_date date,
	ride_time time,
	ride_duration_minutes integer,
	fare_amount numeric (10,2),
	ride_distance_miles numeric (10,2),
	payment_method text,
	voucher text,
	rating numeric (2,0) CHECK (rating BETWEEN 0 AND 5),
	CONSTRAINT fact_unique_composite_key UNIQUE (ride_id, customer_id, driver_id, country)
);

--Creating index for optimization
CREATE INDEX fact_rides_drivers_id ON fact_rides (driver_id);

--SECTION: DATA IMPORT

COPY drivers
FROM 'C:\Users\Renzie\Desktop\SQL2nd ED\kaggle_ride_hailing_app\drivers.csv'
WITH (FORMAT CSV, HEADER, DELIMITER ';');


COPY vouchers 
FROM 'C:\Users\Renzie\Desktop\SQL2nd ED\kaggle_ride_hailing_app\vouchers.csv'
WITH (FORMAT CSV, HEADER);

COPY users 
FROM 'C:\Users\Renzie\Desktop\SQL2nd ED\kaggle_ride_hailing_app\users.csv'
WITH (FORMAT CSV, HEADER, DELIMITER ';');

COPY fact_rides
FROM 'C:\Users\Renzie\Desktop\SQL2nd ED\kaggle_ride_hailing_app\fact_rides.csv'
WITH (FORMAT CSV, HEADER, DELIMITER ';');

--SECTION: CREATING BACKUP TABLES

CREATE TABLE drivers_backup AS
SELECT * FROM drivers;

CREATE TABLE users_backup AS
SELECT * FROM users;

CREATE TABLE vouchers_backup AS
SELECT * FROM vouchers;

CREATE TABLE fact_rides_backup AS
SELECT * FROM fact_rides;


--Section: Data Transformation


--Adding columns to the fact_rides table that will combine ride_date and ride time.
ALTER TABLE fact_rides
	ADD COLUMN  ride_pick_up_time TIMESTAMP,
--Add another column to the fact_rides table as ride_time_departure using ride_duration time column as interval.
	ADD COLUMN  ride_drop_off_time TIMESTAMP;
--Updating ride_pick_up_time and ride_drop_off_time
UPDATE fact_rides
		SET 
			ride_pick_up_time =  ride_date + ride_time,
			ride_drop_off_time = ride_pick_up_time + ride_duration_minutes * INTERVAL '1 minute';
		

--Adding columns to the vouchers table. These will store start_date, end_date, and updated_at as timestamps

ALTER TABLE vouchers
	ADD COLUMN start_date_timestamp TIMESTAMP,
	ADD COLUMN end_date_timestamp TIMESTAMP,
	ADD COLUMN updated_at_timestamp TIMESTAMP;
	
-- Updating the vouchers table by converting Unix timestamps to TIMESTAMP format
SELECT * FROM vouchers
UPDATE vouchers
		SET 
			start_date_timestamp = to_timestamp(start_date),
			end_date_timestamp = to_timestamp(end_date),
			updated_at_timestamp = to_timestamp(updated_at);
			

--Updating the drivers table phone_number column by modifying the rows
	--Update Information: Gisele Zanettini and Rodi Teresa gave out numbers in response to the company's request | 421-636-1380 and 692-522-2222
UPDATE drivers
		SET 
			phone_number = 
				CASE 
					WHEN first_name = 'Gisele' AND last_name = 'Zanettini' THEN '421-636-1380'
					WHEN first_name = 'Rodi' AND last_name = 'Teresa' THEN '692-522-2222'
					ELSE phone_number
				END
		WHERE phone_number IS NULL;
		
--Checking if updated the number				
SELECT first_name, last_name, phone_number FROM drivers
WHERE first_name IN ('Gisele','Rodi') AND last_name IN ('Zanettini','Teresa')

-- END OF CREATING  A DATBASE AND TABLE

--START OF DATA EXPLORATION


--Query 1: What is the number of riders and customers in each country, and what is the ratio of riders to customers?

		WITH ride_stats AS
			(
				SELECT 
					country,
					COUNT(DISTINCT driver_id) AS total_drivers_count,
					COUNT(DISTINCT customer_id) AS total_customers_count,
					round(
							(COUNT(DISTINCT driver_id)/NULLIF (COUNT(DISTINCT customer_id)
							,0)::numeric(10,0) * 1000)
							,0) AS rider_per_1000_customers
				FROM fact_rides
				GROUP BY country
				
			)
		SELECT country,
				total_drivers_count,
				total_customers_count,
				rider_per_1000_customers
		FROM ride_stats
		ORDER BY rider_per_1000_customers
		;
		
--Query 2: What is the busiest time of day? month? day?
--Query 2.1: Busiest time of day
	WITH ride_stats AS (
		SELECT
			country,
			EXTRACT(HOUR FROM ride_pick_up_time) AS time_of_day,
			EXTRACT(MONTH FROM ride_pick_up_time) AS month_of_date,
			EXTRACT(DAY FROM ride_pick_up_time) AS day_of_date,
			COUNT (*) AS total_rides
		FROM fact_rides
		GROUP BY country, time_of_day,month_of_date,day_of_date
		)
	 SELECT  country,
	 		 time_of_day,
			 SUM (total_rides) AS total_rides
	 FROM ride_stats
	 GROUP BY country,time_of_day
	 ORDER BY country,time_of_day,total_rides DESC;
	 
--Query 2.2: Busiest month
	WITH ride_stats AS (
		SELECT
			country,
			EXTRACT(YEAR FROM ride_pick_up_time) AS year_of_date,
			EXTRACT(MONTH FROM ride_pick_up_time) AS month_of_date,
			COUNT (*) AS total_rides
		FROM fact_rides
		GROUP BY country,year_of_date,month_of_date
		)
	 SELECT  country,
	         year_of_date,
	 		 month_of_date,
			 SUM(total_rides) AS total_rides
	 FROM ride_stats
	 GROUP BY country,year_of_date,month_of_date
	 ORDER BY year_of_date,country,month_of_date
--Query 2.3: Busiest day
	WITH ride_stats AS (
		SELECT
			country,
			EXTRACT(DAY FROM ride_pick_up_time) AS day_of_date,
			COUNT (*) AS total_rides
		FROM fact_rides
		GROUP BY country,day_of_date
		)
	 SELECT  country,
	 		 day_of_date,
			 SUM(total_rides) AS total_rides
	 FROM ride_stats
	 GROUP BY country,day_of_date
	 ORDER BY country,day_of_date,total_rides DESC;
	 
--Query 3: Monthly ranking of drivers; per month,cumulative month,per country.
WITH ride_stats AS (
    SELECT
        country,
        driver_id,
        EXTRACT(YEAR FROM ride_pick_up_time) AS year_of_date,
        EXTRACT(MONTH FROM ride_pick_up_time) AS month_of_date,
        COUNT(*) AS total_rides
    FROM fact_rides
    GROUP BY country, driver_id, year_of_date, month_of_date
),
monthly_ranking AS (
    SELECT
        country,
        driver_id,
        year_of_date,
        month_of_date,
        total_rides,
        DENSE_RANK() OVER (PARTITION BY country, year_of_date,month_of_date ORDER BY total_rides DESC) AS monthly_ranking
    FROM ride_stats
),
cumulative_ranking AS  (
	SELECT
		country,
		driver_id,
		year_of_date,
		month_of_date,
		total_rides,
		monthly_ranking,
		SUM(total_rides) OVER (PARTITION BY country,year_of_date,driver_id ORDER BY year_of_date,month_of_date) AS cumulative_rides
 	FROM monthly_ranking
)
	SELECT
		country,
		driver_id,
		year_of_date,
		month_of_date,
		total_rides,
		monthly_ranking,
		cumulative_rides,
--in the cumulative ranking we want to rank the cumulative rides each month it accumulates
		DENSE_RANK() OVER (PARTITION BY country,year_of_date,month_of_date ORDER BY cumulative_rides DESC) AS cumulative_ranking
	FROM cumulative_ranking
	ORDER BY year_of_date,month_of_date,cumulative_ranking;


--Query 4: In our drivers database, is there someone who hasn't started yet with the ride?
--Answer: we have 1000 count of matches. same count with the drivers table
SELECT DISTINCT driver_id
FROM fact_rides
WHERE EXISTS (
		SELECT driver_id
		FROM drivers
		WHERE driver_id = fact_rides.driver_id
);
--Query 4.1
--just to check. lets have another where the result is expected to return nothing or just null since we have a null row in our fact_rides table
--Answer: null value returned
SELECT DISTINCT driver_id
FROM fact_rides
WHERE NOT EXISTS (
		SELECT driver_id
		FROM drivers
		WHERE driver_id = fact_rides.driver_id
);

--Query 5: Top 3 Customers in each country
SELECT  a.country,
		b.first_name,
		b.last_name, 
		b.total_customer_rides,
		b.user_id,
		b.rnk
FROM  (SELECT DISTINCT country FROM fact_rides) a
LEFT JOIN LATERAL(
	SELECT u.first_name,
		   u.last_name,
		   u.user_id,
		   COUNT (*) AS total_customer_rides,
		   DENSE_RANK() OVER (PARTITION BY a.country ORDER BY COUNT (*) DESC) as rnk
		   FROM fact_rides fr
		   LEFT JOIN users u ON
		   fr.customer_id = u.user_id
		   WHERE fr.country = a.country
		   GROUP BY fr.country,u.first_name, u.last_name,u.user_id
		   LIMIT 3 -- putting limit 3 for marketing purposes. we can provide promos or coupons to top 3 customers	   
) b ON true
WHERE a.country IN ('Afghanistan','China','Philippines') 
ORDER BY a.country,  b.total_customer_rides DESC;

--Query 6: In each country what is the most used car make and model 
--Query 6.1: solved with CTE
WITH car_make_count AS 
		(
			SELECT  fr.country AS country,
					d.car_model AS car_model,
					d.car_make AS car_make,
					COUNT (*) as car_make_count_cte,
					ROW_NUMBER () OVER (PARTITION BY fr.country ORDER BY COUNT (*) DESC) 
							AS rnk
			FROM fact_rides fr
			LEFT JOIN drivers d ON
			d.driver_id = fr.driver_id
			GROUP BY fr.country, d.car_make, d.car_model
		)
SELECT  country,
		car_make,
		car_model,
		car_make_count_cte,
	    rnk
FROM car_make_count
WHERE rnk = 1
ORDER BY country,rnk,car_make_count_cte DESC

--Query 6.2:Solved using LATERAL JOIN

SELECT a.country,
	   b.car_make,
	   b.car_model, 
	   b.car_make_count
FROM  (SELECT DISTINCT country FROM fact_rides) a
LEFT JOIN LATERAL(
	SELECT d.car_make,
		   d.car_model,
		   COUNT (*) AS car_make_count
		   FROM fact_rides fr
		   LEFT JOIN drivers d ON
		   fr.driver_id = d.driver_id
		   WHERE fr.country = a.country
		   GROUP BY d.car_make, d.car_model
		   ORDER BY car_make_count DESC
		   LIMIT 1
) b ON true
ORDER BY a.country;
 			
--Query 6.3:Solved using LATERAL JOIN
---Ranking using row number and lateral joins and putting inside a query

SELECT country,
       car_make,
       car_model,
       car_make_count,
	   row_rank
FROM (
		    SELECT a.country,
		           b.car_make,
		           b.car_model,
		           b.car_make_count,
		           ROW_NUMBER() OVER (PARTITION BY a.country ORDER BY b.car_make_count DESC) AS row_rank
		    FROM (SELECT DISTINCT country FROM fact_rides) a
		    LEFT JOIN LATERAL (
		                        SELECT d.car_make,
		                               d.car_model,
		                               COUNT(*) AS car_make_count
		                        FROM fact_rides fr
		                        LEFT JOIN drivers d ON
		                               d.driver_id = fr.driver_id
		                        WHERE fr.country = a.country -- this binds it to the initial statement
		                        GROUP BY d.car_make, d.car_model
		                        ORDER BY car_make_count DESC
		                      ) b ON true
) subquery
WHERE row_rank = 1;

--Query 7: Active riders per month and if there is a demand
	  WITH ride_stats AS
			(
				SELECT 
					country,
					EXTRACT (MONTH FROM ride_pick_up_time) AS month_number,
					EXTRACT (YEAR FROM ride_pick_up_time) AS year_number,
					COUNT(DISTINCT driver_id) AS total_drivers_count,
					COUNT(DISTINCT customer_id) AS total_customers_count,
					round((COUNT(DISTINCT driver_id)/NULLIF (COUNT(DISTINCT customer_id),0)::numeric(10,0) * 1000),0) AS rider_per_1000_customers
				FROM fact_rides
				GROUP BY country,month_number,year_number
				ORDER BY rider_per_1000_customers
			)
		SELECT country,
		        month_number,
				year_number,
				total_drivers_count,
				total_customers_count,
				rider_per_1000_customers
		FROM ride_stats
		WHERE  country = 'China'
	    ORDER BY country,year_number, month_number,rider_per_1000_customers DESC
		;



--Query 8: Active riders per month and if there is a demand
--
SELECT country,
	EXTRACT (HOUR FROM ride_pick_up_time) AS trip_hour,
	percentile_cont(.5)
		WITHIN GROUP (ORDER BY 
					ride_drop_off_time - ride_pick_up_time) AS median_trip
	 
FROM fact_rides
WHERE country IN ('Philippines')
GROUP BY country,trip_hour
ORDER BY country,trip_hour;
 
--Query 9:Vouchers. 

--Query 9.1: Vouchers Usage Trends
	WITH vouchers_usage AS 
	(
	SELECT 
		EXTRACT (MONTH FROM ride_pick_up_time) AS month_,
		EXTRACT (YEAR FROM ride_pick_up_time) AS  year_,
		COUNT (*) AS total_rides,
		COUNT(
				CASE WHEN voucher IS NOT NULL THEN 1
				END) AS voucher_rides,
		round((COUNT(CASE WHEN voucher IS NOT NULL THEN 1
				END) * 100 /COUNT(*)),2) AS voucher_percentage
	 FROM fact_rides
	 GROUP BY year_,month_ 
	 ORDER BY year_,month_)

	 SELECT 
	 		 year_,
			 month_,
		 	voucher_rides,
		 	voucher_percentage
	FROM vouchers_usage
	 
--Query 9.2: Vouchers Usage Trends		
    

--Query 9.3: Do voucher users ride more often

WITH customer_ride_count AS 
(
	SELECT
		country,
		customer_id,
		EXTRACT(YEAR FROM ride_pick_up_time) AS year_,
		EXTRACT(MONTH FROM ride_pick_up_time)AS month_,
		COUNT (*) AS total_rides,
		COUNT(CASE WHEN voucher IS NOT NULL THEN 1 END) AS voucher_rides
	FROM fact_rides
	GROUP BY year_,month_,country,customer_id
)
SELECT
	year_,
	month_,
	country,
	CASE WHEN voucher_rides > 0 THEN 'Used Voucher' ELSE 'No Voucher' END customer_type,
	COUNT(*) AS customer_count
	FROM customer_ride_count
	WHERE country = 'Philippines'
	GROUP BY year_,month_,country,customer_type
	ORDER BY year_,country,month_,customer_type

--Query 9.4: Do vouchers increase overall revenue
SELECT * FROM fact_rides
CREATE VIEW vw_monthly_ride_summary AS
SELECT 
	country,
	EXTRACT(YEAR FROM ride_pick_up_time) AS year_,
	EXTRACT(MONTH FROM ride_pick_up_time)AS month_,
	CASE WHEN voucher IS NOT NULL THEN 'Used Voucher' ELSE 'No Voucher' END customer_type,
	ROUND(SUM(fare_amount),2) AS revenue,
	--note:we use average when data is symmetrically distributed
	ROUND(AVG(fare_amount),2) AS avg_fare_per_ride,
	--note: use median when the data has outliers or is skewed
	ROUND(PERCENTILE_CONT(.5) WITHIN GROUP (ORDER BY fare_amount):: numeric(10,2),2) AS median_average
FROM fact_rides
WHERE country = 'Afghanistan' AND EXTRACT(MONTH FROM ride_pick_up_time) = 2
GROUP BY country,year_,month_,customer_type;

DROP VIEW IF EXISTS vw_monthly_ride_summary;

