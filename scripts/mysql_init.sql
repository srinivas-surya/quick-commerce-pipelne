-- Create orders table and seed from sample CSV (simple demo)
CREATE TABLE IF NOT EXISTS orders (
  order_id BIGINT PRIMARY KEY,
  user_id BIGINT,
  item_id INT,
  quantity INT,
  price DECIMAL(10,2),
  order_ts TIMESTAMP,
  status VARCHAR(20)
);

LOAD DATA INFILE '/samples/orders_sample.csv'
INTO TABLE orders
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(order_id,user_id,item_id,quantity,price,@order_ts,status)
SET order_ts = STR_TO_DATE(@order_ts, '%Y-%m-%d %H:%i:%s');
