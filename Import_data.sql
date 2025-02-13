CREATE DATABASE Taobao;
USE Taobao;

DROP TABLE IF EXISTS user_behavior_temp;
CREATE TABLE user_behavior_temp (
    user_id INT, 
    item_id INT, 
    category_id INT, 
    behavior_type VARCHAR(10), 
    time_stamp INT
);

LOAD DATA LOCAL INFILE "/tmp/UserBehavior.csv"
INTO TABLE user_behavior_temp
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

DROP TABLE IF EXISTS user_behavior;
CREATE TABLE user_behavior (
    user_id INT, 
    item_id INT, 
    category_id INT, 
    behavior_type VARCHAR(10), 
    time_stamp INT
);

LOAD DATA LOCAL INFILE "/tmp/UserBehavior_10M.csv"
INTO TABLE user_behavior
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


