SET GLOBAL net_read_timeout = 1200;  
SET GLOBAL net_write_timeout = 1200; 
SET GLOBAL wait_timeout = 86400;     
SET GLOBAL interactive_timeout = 86400;
SET GLOBAL max_allowed_packet = 1073741824; -- 1GB max packet size
SET GLOBAL innodb_buffer_pool_size = 17179869184; -- 16GB

-- Check Missing Values--
SELECT *
FROM user_behavior
WHERE user_id IS NULL
    OR item_id IS NULL
    OR category_id IS NULL
    OR behavior_type IS NULL
    OR time_stamp IS NULL;

-- Check Duplicated values --
CREATE INDEX idx_user_item_time ON user_behavior (user_id, item_id, time_stamp);
SELECT
	user_id, item_id, time_stamp
FROM user_behavior
GROUP BY user_id, item_id, time_stamp
HAVING COUNT(*)>1;

CREATE VIEW duplicate_value AS
SELECT
	user_id, item_id, time_stamp
FROM user_behavior
GROUP BY user_id, item_id, time_stamp
HAVING COUNT(*)>1;

SELECT
	COUNT(*)
FROM duplicate_value; -- Number of duplicated data = 5 -- 

-- Delete Duplication--
ALTER TABLE user_behavior ADD id INT FIRST;
ALTER TABLE user_behavior MODIFY id INT AUTO_INCREMENT PRIMARY KEY;

DELETE ub
FROM user_behavior ub
JOIN (
    SELECT user_id, item_id, time_stamp, MAX(id) AS max_id
    FROM user_behavior
    GROUP BY user_id, item_id, time_stamp
    HAVING COUNT(*) > 1
) AS df1
ON ub.user_id = df1.user_id
AND ub.item_id = df1.item_id
AND ub.time_stamp = df1.time_stamp
WHERE ub.id < df1.max_id;

-- Time Range Setting -- 
ALTER TABLE user_behavior ADD datetimes TIMESTAMP (0);
CREATE INDEX idx_datetimes ON user_behavior(datetimes);
CREATE INDEX idx_time ON user_behavior(time_stamp);

UPDATE user_behavior 
SET datetimes = DATE_ADD(FROM_UNIXTIME(time_stamp), INTERVAL 7 HOUR);
DELETE FROM user_behavior
WHERE datetimes > '2017-12-03 23:59:59'
OR datetimes < '2017-11-25 00:00:00'
Or datetimes IS NULL;

SELECT COUNT(*) FROM user_behavior; -- ROW number check--

ALTER TABLE user_behavior ADD dates DATE;
ALTER TABLE user_behavior ADD hours TINYINT UNSIGNED;
ALTER TABLE user_behavior ADD INDEX idx_date (dates);
ALTER TABLE user_behavior ADD INDEX idx_hour (hours);
UPDATE user_behavior
SET dates = DATE(datetimes),
    hours = HOUR(datetimes);
    
-- User Acquisition --
DROP TABLE IF EXISTS df_pv_uv;
CREATE TABLE df_pv_uv (
    dates DATE,
    PV INT UNSIGNED,
    UV INT UNSIGNED,
    PVUV DECIMAL(10, 2) 
);
ALTER TABLE user_behavior ADD INDEX idx_behavior_type (behavior_type);
CREATE INDEX idx_behavior_user ON user_behavior(behavior_type, user_id);
CREATE INDEX idx_behavior_date ON user_behavior(dates, behavior_type);

INSERT INTO df_pv_uv (dates, PV, UV, PVUV)
SELECT dates,
       SUM(CASE WHEN behavior_type = 'pv' THEN 1 ELSE 0 END) AS PV,
       COUNT(DISTINCT user_id) AS UV,
       ROUND(SUM(CASE WHEN behavior_type = 'pv' THEN 1 ELSE 0 END) / COUNT(DISTINCT user_id), 2) AS PVUV
FROM user_behavior
GROUP BY dates;

-- User Retention --

DROP TABLE IF EXISTS df_retention_1; -- Next Day Retention
CREATE TABLE df_retention_1 (
    dates DATE, 
    retention_1 FLOAT
);

CREATE INDEX idx_user_dates ON user_behavior(user_id, dates);

INSERT INTO df_retention_1
SELECT
    ub_1.dates,
    COUNT(ub_2.user_id) / COUNT(ub_1.user_id) AS retention_1
FROM 
    (SELECT user_id, dates FROM user_behavior GROUP BY user_id, dates) AS ub_1
LEFT JOIN 
    (SELECT user_id, DATE_ADD(dates, INTERVAL -1 DAY) AS prev_date FROM user_behavior GROUP BY user_id, dates) AS ub_2
ON ub_1.user_id = ub_2.user_id
AND ub_1.dates = ub_2.prev_date
GROUP BY ub_1.dates;

DROP TABLE IF EXISTS df_retention_3; -- Next Day Retention
CREATE TABLE df_retention_3 ( -- 3 days retention--
    dates DATE, 
    retention_3 FLOAT
);

INSERT INTO df_retention_3
SELECT
    ub_1.dates,
    COUNT(ub_2.user_id) / COUNT(ub_1.user_id) AS retention_1
FROM 
    (SELECT user_id, dates FROM user_behavior GROUP BY user_id, dates) AS ub_1
LEFT JOIN 
    (SELECT user_id, DATE_ADD(dates, INTERVAL -3 DAY) AS prev_date FROM user_behavior GROUP BY user_id, dates) AS ub_2
ON ub_1.user_id = ub_2.user_id
AND ub_1.dates = ub_2.prev_date
GROUP BY ub_1.dates;

-- User Behavior--
DROP TABLE IF EXISTS df_timeseries;
CREATE TABLE df_timeseries (
    dates DATE,
    hours TINYINT UNSIGNED,
    PV INT UNSIGNED,
    CART INT UNSIGNED,
    FAV INT UNSIGNED,
    BUY INT UNSIGNED
);

INSERT INTO df_timeseries
SELECT 
    dates,
    hours,
    SUM(CASE WHEN behavior_type = 'pv' THEN 1 ELSE 0 END) AS PV,
    SUM(CASE WHEN behavior_type = 'cart' THEN 1 ELSE 0 END) AS CART,
    SUM(CASE WHEN behavior_type = 'fav' THEN 1 ELSE 0 END) AS FAV,
    SUM(CASE WHEN behavior_type = 'buy' THEN 1 ELSE 0 END) AS BUY
FROM user_behavior
GROUP BY dates, hours
ORDER BY dates ASC, hours ASC;



-- User Behavior Conversion After Viewing --
CREATE INDEX idx_behavior ON user_behavior(user_id, item_id, behavior_type);

CREATE VIEW user_behavior_total AS -- Check the real behaviors -- 
SELECT
    user_id,
    item_id,
    SUM(CASE WHEN behavior_type = 'pv' THEN 1 ELSE 0 END) AS PV,
    SUM(CASE WHEN behavior_type = 'fav' THEN 1 ELSE 0 END) AS FAV,
    SUM(CASE WHEN behavior_type = 'cart' THEN 1 ELSE 0 END) AS CART,
    SUM(CASE WHEN behavior_type = 'buy' THEN 1 ELSE 0 END) AS BUY
FROM user_behavior
GROUP BY user_id, item_id;

CREATE VIEW user_behavior_total_standard AS -- Behavior standardization -- 
SELECT
    user_id,
    item_id,
    SIGN(PV) AS ifpv,       -- 1 if PV > 0, otherwise 0
    SIGN(FAV) AS iffav,     -- 1 if FAV > 0, otherwise 0
    SIGN(CART) AS ifcart,   -- 1 if CART > 0, otherwise 0
    SIGN(BUY) AS ifbuy      -- 1 if BUY > 0, otherwise 0
FROM user_behavior_total;

CREATE VIEW user_path AS  -- user behavior on a certain item is recorded as path
SELECT
    user_id,
    item_id,
    CONCAT(ifpv, iffav, ifcart, ifbuy) AS path
FROM user_behavior_total_standard;

CREATE VIEW user_path_num AS -- 
SELECT 
    path,
    CASE 
        WHEN path = '1101' THEN 'View-Favorite-/-Order'
        WHEN path = '1011' THEN 'View-/-Cart-Order'
        WHEN path = '1111' THEN 'View-Favorite-Cart-Order'
        WHEN path = '1001' THEN 'View-/-/-Order'
        WHEN path = '1010' THEN 'View-/-Cart-/' 
        WHEN path = '1100' THEN 'View-Favorite-/-/' 
        WHEN path = '1110' THEN 'View-Favorite-Cart-/' 
        ELSE 'View-/-/-/' 
    END AS description,
    COUNT(*) AS path_num
FROM user_path
WHERE path REGEXP '^1'
GROUP BY path;

DROP TABLE IF EXISTS df_buy_path;
CREATE TABLE df_buy_path
(	buy_path VARCHAR(55),
	buy_path_num INT
);

INSERT INTO df_buy_path
SELECT 
	'View',
    sum(path_num) AS buy_path_num
FROM user_path_num;

INSERT INTO df_buy_path
SELECT 
	'Favorite and Cart after View',
    sum(IF(path = 1101
		OR path = 1100
        OR path = 1010
        OR path = 1011
        OR path = 1110
        OR path = 1111,
        path_num, NULL
		)) AS buy_path_num
FROM user_path_num;

INSERT INTO df_buy_path
SELECT 
	'Order after View, Favorit, and Cart',
    sum(IF(path = 1101
		OR path = 1011
        OR path = 1111,
        path_num, NULL
		)) AS buy_path_num
FROM user_path_num;

-- User Positioning: RFM model --
DROP VIEW IF EXISTS c;
CREATE VIEW c AS -- R --
SELECT
	user_id,
    max(dates) AS last_buy_date
FROM user_behavior
WHERE behavior_type = 'buy'
GROUP BY user_id;

DROP VIEW IF EXISTS d;
CREATE VIEW d AS -- F --
SELECT 
	user_id,
    count(user_id) AS buy_times
FROM user_behavior
WHERE behavior_type = 'buy'
GROUP BY user_id;

DROP TABLE IF EXISTS df_rfm_model;
CREATE TABLE df_rfm_model
( 	user_id INT,
	recency DATE,
    frequency INT
);
INSERT INTO df_rfm_model
SELECT 
    c.user_id, 
    c.last_buy_date, 
    d.buy_times  -- Corrected column name
FROM c
JOIN d USING (user_id);

ALTER TABLE df_rfm_model ADD r_score INT;
UPDATE df_rfm_model
SET r_score =
    CASE
		WHEN recency = '2017-12-03' THEN 100
        WHEN recency = '2017-12-02' OR recency = '2017-12-01' THEN 80
        WHEN recency = '2017-11-30' OR recency = '2017-11-29' THEN 60
		WHEN recency = '2017-11-28' OR recency = '2017-11-27' THEN 40
        ELSE 20
	END;

ALTER TABLE df_rfm_model ADD f_score INT;
UPDATE df_rfm_model
SET f_score =
    CASE 
        WHEN frequency > 15 THEN 100
        WHEN frequency BETWEEN 12 AND 14 THEN 90
        WHEN frequency BETWEEN 9 AND 11 THEN 70
        WHEN frequency BETWEEN 6 AND 8 THEN 50
        WHEN frequency BETWEEN 3 AND 5 THEN 30
        ELSE 10
    END;

DROP TABLE IF EXISTS df_rfm_avg;
CREATE TABLE df_rfm_avg
(	user_id INT,
	recency DATE,
    r_score INT,
    avg_r DECIMAL(6,4),
    frequency INT,
    f_score INT,
    avg_f DECIMAL(6,4)
);

INSERT INTO df_rfm_avg
SELECT
	e.user_id as user_id,
    recency,
    r_score,
    avg_r,
    frequency,
    f_score,
    avg_f
FROM ( SELECT
		user_id,
        AVG(r_score) OVER () AS avg_r,
        AVG(f_score) OVER () AS avg_f
	    FROM df_rfm_model) AS e
JOIN df_rfm_model USING (user_id);

DROP TABLE IF EXISTS df_rfm_result;
CREATE TABLE df_rfm_result
(	user_class varchar(55),
	user_calss_num INT
);

INSERT INTO df_rfm_result
SELECT
	user_class,
    COUNT(*) AS user_class_num
FROM(SELECT
		CASE 
			WHEN (f_score >= avg_f AND r_score >= avg_r) THEN 'Loyal & Active User'
			WHEN (f_score >= avg_f AND r_score < avg_r) THEN 'Frequent but Inactive User'
            WHEN (f_score < avg_f AND r_score >= avg_r) THEN 'New/Potential User'
            ELSE 'Churned User' -- Low Frequency, Low Recency (Retention Needed)
            END AS user_class
		FROM df_rfm_avg
			) AS g
GROUP BY user_class;

-- Product Analysis --
DROP TABLE IF EXISTS df_popular_category; -- Top 10 viewed categories --
CREATE TABLE df_popular_category 
(	category_id INT,
	category_pv INT
);

INSERT INTO df_popular_category
SELECT
	category_id,
    count(IF(behavior_type='pv',1,NULL)) AS category_pv
FROM user_behavior
GROUP BY category_id
ORDER BY count(IF(behavior_type='pv',1,NULL)) DESC
LIMIT 10;

DROP TABLE IF EXISTS df_popular_item; -- Top 10 viewed items --
CREATE TABLE df_popular_item
(	item_id INT,
	item_pv INT
);

INSERT INTO df_popular_item
SELECT
	item_id,
    count(IF(behavior_type='pv',1,NULL)) AS item_pv
FROM user_behavior
GROUP BY item_id
ORDER BY count(IF(behavior_type='pv',1,NULL)) DESC
LIMIT 10;

DROP TABLE IF EXISTS df_category_conv_rate; -- Product Feture --
CREATE TABLE df_category_conv_rate 
(	category_id INT,
	PV INT,
    FAV INT,
    CART INT,
    BUY INT,
    category_conv_rate FLOAT
);
INSERT INTO df_category_conv_rate
SELECT
    category_id,
    SUM(CASE WHEN behavior_type = 'pv' THEN 1 ELSE 0 END) AS PV,
    SUM(CASE WHEN behavior_type = 'fav' THEN 1 ELSE 0 END) AS FAV,
    SUM(CASE WHEN behavior_type = 'cart' THEN 1 ELSE 0 END) AS CART,
    SUM(CASE WHEN behavior_type = 'buy' THEN 1 ELSE 0 END) AS BUY,
    COUNT(DISTINCT IF(behavior_type = 'buy', user_id,NULL))/COUNT(DISTINCT user_id) AS category_conv_rate
FROM user_behavior
GROUP BY category_id
ORDER BY category_conv_rate DESC;