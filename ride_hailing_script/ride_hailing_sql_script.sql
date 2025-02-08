------------------------------------------------------------------------------------------------
-- Ride Hailing App Project
-- by Renzie Reyes
-- source: https://www.kaggle.com/datasets/galihwardiana/ride-hailing-transaction
------------------------------------------------------------------------------------------------


--Listing 1-1: Creating a database named ride_hailing_analysis


CREATE DATABASE ride_hailing_analysis;


--Listing 1-2: Creating tables for drivers, users, vouchers, fact_rides

--Drivers
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

--Users
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

CREATE TABLE vouchers (
	voucher_code text,
	discount_amount numeric (10,2),
	start_date bigint,
	end_date bigint,
	updated_at bigint,
	discount_perc numeric (5,2)
);

--Alter column type of vouchers table
--we altered the discount amount and discount percentage just to be sure
DROP TABLE vouchers
ALTER TABLE vouchers
	ALTER COLUMN discount_amount TYPE numeric (10,2),
	ALTER COLUMN discount_perc TYPE numeric (5,2),
	ALTER COLUMN start_date_unix TYPE TIMESTAMP,
	ALTER COLUMN ended_date_unix TYPE TIMESTAMP,
	ALTER COLUMN updated_at_date_unix TYPE TIMESTAMP,
	ALTER COLUMN start_date TYPE BIGINT,
	ALTER COLUMN end_date TYPE  BIGINT,
	ALTER COLUMN updated_at TYPE BIGINT;

ALTER TABLE vouchers
	ALTER COLUMN updated_at TYPE BIGINT;

SELECT * FROM vouchers;

CREATE TABLE fact_rides (
	ride_id
	customer_id
	driver_id
	start_location
	end_location
	city
	country
	ride_date
	ride_time
	ride_duration_minutes
	fare_amount
	ride_distance_miles
	payment_method
	voucher
	rating
);
--Listing 1-3: Importing data
--the drivers.csv file has a delimiter of ';' thus we add a delimiter ';' after the header

COPY drivers
FROM 'C:\Users\Renzie\Desktop\SQL2nd ED\kaggle_ride_hailing_app\drivers.csv'
WITH (FORMAT CSV, HEADER, DELIMITER ';');

--the vouchers.csv file has a delimiter of ','. this is the default
COPY vouchers 
FROM 'C:\Users\Renzie\Desktop\SQL2nd ED\kaggle_ride_hailing_app\vouchers.csv'
WITH (FORMAT CSV, HEADER);


SELECT to_timestamp(start_date) FROM vouchers


--Listing 1-4: 
--Checking of null per column because we want to make sure that each driver has a phone number as much as possible
--we found out the there are 40 driver's that do not have phone numbers

SELECT first_name, last_name, phone_number FROM drivers 
WHERE phone_number IS NULL;

	--next is we notify them if they can provide 
	--lu


