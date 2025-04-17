--CREATE DATABASE videogames_data;

CREATE TABLE fact_data (
	img VARCHAR(500),
	title  TEXT,
	console VARCHAR(20),
	genre VARCHAR(20),
	publisher VARCHAR(500),
	developer VARCHAR(500),
	critic_score DECIMAL(5,2),
	total_sales NUMERIC(5,2),
	na_sales NUMERIC(5,2),
	jp_sales NUMERIC(5,2),
	pal_sales NUMERIC(5,2),
	other_sales NUMERIC(5,2),
	release_date DATE,
	last_update DATE
)


--UPDATE img coplumn to its fullpath
ALTER TABLE fact_data ADD COLUMN full_image_url TEXT;

UPDATE fact_data
SET img = CONCAT('https://www.vgchartz.com', img);

UPDATE fact_data
SET full_image_url = CONCAT('https://www.vgchartz.com', img);

--DROP TABLE fact_data

COPY fact_data
FROM 'C:\Users\Renzie\Desktop\SQL2nd ED\videogames_daashboaard\Test.csv'
WITH (FORMAT CSV, HEADER);

SELECT * FROM fact_data
--Cleaning Fact_table (null as "Unknown")
CREATE VIEW cleaned_fact_data AS
SELECT *, 
COALESCE(developer,'Unknown') AS cleaned_developer,
COALESCE(na_sales,0) + COALESCE(jp_sales,0) + COALESCE(pal_sales,0) + COALESCE(other_sales,0) AS new_total_sales,
--logic: if developer is null, replace with unknown.
--logic2 : if the sales amount is zero then the game is a free game
CASE
	WHEN 
	 (COALESCE(na_sales,0) + COALESCE(jp_sales,0) + COALESCE(pal_sales,0) + COALESCE(other_sales,0)) = 0 THEN 'Free game'
	ELSE 'Paid game'
END AS paid_or_free_type,
--Cleaning Fact_table (Cleaning Duplicates)
--in order to build the star schema,we cleaned the subtle duplicates
CASE 
	WHEN developer ILIKE 'chunsoft' THEN 'ChunSoft'
	WHEN developer ILIKE '5th Cell' THEN '5th Cell'
	WHEN developer ILIKE 'Deck13 Interactive' THEN 'Deck13 Interactive'
	WHEN developer ILIKE 'MixedBag Srl' THEN 'MixedBag Srl'
	WHEN developer ILIKE 'Red Entertainment' THEN 'Red Entertainment'
	WHEN developer ILIKE 'SPECTRUM HOLOBYTE' THEN 'Spectrum Holobyte'
	WHEN developer ILIKE 'TOSE' THEN 'TOSE'
	WHEN developer ILIKE 'YAK & cO' THEN 'YAK & cO'
ELSE developer
END AS developer_name_cleaned
FROM fact_data
-- WHERE CASE 
-- 	WHEN (publisher = 'Unknown' OR  COALESCE(developer,'Unknown') = 'Unknown')
-- 			AND (COALESCE(na_sales,0) + COALESCE(jp_sales,0) + COALESCE(pal_sales,0) + COALESCE(other_sales,0)) = 0 THEN 'Free game'
-- 	ELSE 'Paid game'
-- END  = 'Free game'

DROP VIEW cleaned_fact_data
--CREATE dim_console
CREATE VIEW dim_console AS
SELECT DISTINCT console AS console_name
FROM fact_data


--CREATE dim_genre
CREATE VIEW dim_genre AS
SELECT DISTINCT genre AS genre_name
FROM fact_data
--
--CREATE dim_publisher
CREATE VIEW dim_publisher AS
SELECT DISTINCT publisher AS publisher_name
FROM fact_data

--CREATE dim_developer
--DROP VIEW dim_developer
CREATE VIEW dim_developer ASB`
SELECT DISTINCT developer_name_cleaned FROM cleaned_fact_data
WHERE developer_name_cleaned IS NOT NULL

SELECT * FROM cleaned_fact_data

CREATE VIEW dim_imgfullpath AS
SELECT DISTINCT img FROM cleaned_fact_data
WHERE img IS NOT NULL
--DROP VIEW dim_imgfullpath
--DROP VIEW cleaned_fact_data


SELECT * FROM cleaned_fact_data
WHERE paid_or_free_type = 'Free game'

-----------------------------------------------March 21, 2025----Normalization--------
--Normalization
CREATE TABLE video_games_normalized (
    game_id SERIAL PRIMARY KEY,
	img TEXT ,
    title TEXT,
	console TEXT,
    publisher TEXT,  -- Region instead of separate columns
    developer TEXT,
	UNIQUE (img,title,console,publisher,developer)
);

INSERT INTO video_games_normalized (img,title,publisher,console,developer)
SELECT DISTINCT img, title,console, publisher, developer
FROM cleaned_fact_data
ON CONFLICT (img,title,console,publisher,developer) DO NOTHING;

--to check if there img allows duplicate when the combination of img,title,console,publisher,developer is different
SELECT * FROM video_games_normalized
WHERE img ILIKE '%default%'


--deleted the previous content of the table video_games_sales_normalized as I decided to apply COALESCE in the INSERT INTO stage of creating a table. 
DELETE FROM video_games_normalized

SELECT * FROM video_games_sales_normalized

SELECT SUM(sales) FROM video_games_sales_normalized


DROP TABLE video_games_sales_normalized

SELECT * FROM cleaned_fact_data

--Normalization version 2
CREATE TABLE publishers (
	publishers_id SERIAL PRIMARY KEY,
	publisher_name TEXT UNIQUE
	);

INSERT INTO publishers (publisher_name)
SELECT DISTINCT publisher FROM cleaned_fact_data
ON CONFLICT (publisher_name) DO NOTHING;

CREATE TABLE developers (
	developer_id SERIAL PRIMARY KEY,
	developer_name TEXT UNIQUE
);

INSERT INTO developers (developer_name)
SELECT DISTINCT developer FROM cleaned_fact_data
ON CONFLICT (developer_name) DO NOTHING;

CREATE TABLE genres (
	genres_id SERIAL PRIMARY KEY,
	genre_name TEXT UNIQUE
);

INSERT INTO genres (genre_name)
SELECT DISTINCT genre FROM cleaned_fact_data
ON CONFLICT (genre_name) DO NOTHING;

--video_games_n table
CREATE TABLE video_games_n (
	game_id SERIAL PRIMARY KEY,
	img TEXT,
	title TEXT,
	console TEXT,
	publisher TEXT,
	developer TEXT,
	critic_score NUMERIC(5,2),
	release_date DATE,
	last_update DATE,
	UNIQUE (img,title,console,publisher,developer));

INSERT INTO video_games_n (img,title,console,publisher,developer,critic_score,release_date,last_update)
SELECT DISTINCT img, title, console, publisher, developer, critic_score,release_date,last_update
FROM cleaned_fact_data
ON CONFLICT (img,title, console,publisher, developer) DO NOTHING;

SELECT * FROM video_games_n
--CALENDAR TABLE

CREATE TABLE CALENDAR (
	date_id SERIAL PRIMARY KEY ,
	full_date DATE UNIQUE NOT NULL,
	year INT GENERATED ALWAYS AS (EXTRACT (YEAR FROM full_date)) STORED,
	month INT GENERATED ALWAYS AS (EXTRACT (MONTH FROM full_date)) STORED,
	day INT GENERATED ALWAYS AS (EXTRACT (DAY FROM full_date)) STORED,
	quarter INT GENERATED ALWAYS AS (EXTRACT (QUARTER FROM full_date)) STORED
);


INSERT INTO calendar(full_date)
SELECT generate_series(
	(SELECT MIN(release_date) FROM cleaned_fact_data WHERE release_date IS NOT NULL),
	(SELECT MAX(last_update) FROM cleaned_fact_data WHERE release_date IS NOT NULL),
	INTERVAL '1 day'
)::DATE;

--Video games sales table
CREATE TABLE video_games_sales_n (
	sales_id SERIAL PRIMARY KEY,
	game_id INT REFERENCES video_games_n(game_id),
	region TEXT CHECK (region IN('NA','JP','EU','Other')),
	sales NUMERIC(10,3),
	date_id INT REFERENCES calendar (date_id) ON DELETE CASCADE
);
 
INSERT INTO video_games_sales_n (game_id,region,sales,date_id)--date_id
SELECT
	vg.game_id,
	unnest(ARRAY['NA','JP','EU','Other']) AS region,
	unnest(ARRAY[COALESCE(na_sales,0),COALESCE(jp_sales,0),COALESCE(pal_sales,0),COALESCE(other_sales,0)]) AS sales,
	COALESCE(c.date_id,(SELECT date_id FROM calendar WHERE full_date = '1900-01-01'))  -- Assign placeholder date because some of the release dates are null
FROM cleaned_fact_data cfd
JOIN video_games_n vg ON cfd.title = vg.title AND cfd.console = vg.console AND cfd.img= vg.img
LEFT JOIN calendar c
	ON cfd.release_date = c.full_date

--DELETE FROM video_games_sales_n


SELECT SUM(sales) FROM video_games_sales_n 
WHERE region = 'NA'

--now they are the same
SELECT SUM(COALESCE(na_sales,0)) FROM cleaned_fact_data

SELECT * FROM cleaned_fact_data

SELECT SUM(sales) FROM video_games_sales_n 

WITH dev_sales AS (
    SELECT d.developer_name, SUM(vgns.sales) AS total_sales
    FROM developers d
    LEFT JOIN video_games_n vgn ON vgn.developer = d.developer_name
    LEFT JOIN video_games_sales_n vgns ON vgns.game_id = vgn.game_id
    WHERE vgn.release_date IS NOT NULL
    GROUP BY d.developer_name
)
SELECT DISTINCT game_id,SUM(sales) FROM public.video_games_n
WHERE game 

SELECT  developer,game_id FROM video_games_n GROUP BY developer,game_id ORDER BY developer desc
SELECT * FROm video_games_n
SELECT DISTINCT game_id, SUM(sales) FROM public.video_games_sales_n
GROUP BY game_id ORDER BY game_id desc
SELECT * FROM video_games_sales_n

WITH dev_sales AS (
SELECT  COALESCE(vgns.region,'missing') AS region_name,COALESCE(vgn.developer,'missing') AS developer_name, SUM(vgns.sales) AS sales FROM developers d
JOIN video_games_n vgn ON COALESCE(vgn.developer,'missing') = COALESCE(d.developer_name,'missing')
JOIN video_games_sales_n vgns ON vgns.game_id = vgn.game_id
GROUP BY vgns.region,vgn.developer
),
dev_sales_2 AS (
SELECT sales FROM dev_sales
)
SELECT  SUM(sales)
FROM dev_sales_2
ORDER BY sales DESC;

