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
--Vouchers
CREATE TABLE vouchers (
	voucher_code text,
	discount_amount numeric (10,2),
	start_date bigint,
	end_date bigint,
	updated_at bigint,
	discount_perc numeric (5,2)
);

--Fact_rides
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
FROM 'C:\Users\Asus\Desktop\sql_portfolio_projects-main\kaggle_ride_hailing_app\drivers.csv'
--FROM 'C:\Users\Renzie\Desktop\SQL2nd ED\kaggle_ride_hailing_app\drivers.csv'
WITH (FORMAT CSV, HEADER, DELIMITER ';');

--the vouchers.csv file has a delimiter of ','. this is the default
COPY vouchers 
FROM 'C:\Users\Asus\Desktop\sql_portfolio_projects-main\kaggle_ride_hailing_app\vouchers.csv'
WITH (FORMAT CSV, HEADER);

----the userss.csv file has a delimiter of ';' thus we add a delimiter ';' after the header
COPY users 
FROM 'C:\Users\Asus\Desktop\sql_portfolio_projects-main\kaggle_ride_hailing_app\users.csv'
WITH (FORMAT CSV, HEADER, DELIMITER ';');


--Modification on vouchers table

ALTER TABLE vouchers
	ALTER COLUMN discount_amount TYPE numeric (10,2),
	ALTER COLUMN discount_perc TYPE numeric (5,2);
	
--need to transform the start_date,end_ date,updated_at dates to dates
--added three new column to store the converted dates: from BIGINT into TIMESTAMP

SELECT * FROM vouchers;
ALTER TABLE vouchers
	ADD COLUMN start_date_timestamp TIMESTAMP,
	ADD COLUMN end_date_timestamp TIMESTAMP,
	ADD COLUMN updated_at_timestamp TIMESTAMP;
-- Updating the vouchers table by converting Unix timestamps to TIMESTAMP format

UPDATE vouchers
		SET 
			start_date_timestamp = to_timestamp(start_date),
			end_date_timestamp = to_timestamp(end_date),
			updated_at_timestamp = to_timestamp(updated_at);

SELECT * FROM users;

--Listing 1-4: 
--Checking of null per column because we want to make sure that each driver has a phone number
--we found out the there are 40 drivers that do not have phone numbers

SELECT first_name, last_name, phone_number FROM drivers 
WHERE phone_number IS NULL;

--next is we notify them if they can provide 
--Gisele Zanettini and Rodi Teresa gave out numbers in response to the company's request
--421-636-1380 and 692-522-2222
--Updating the drivers table phone_number column by modifying the rows

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
