Hive Assignment 1 
=======================================================================================================================
Question 1: Create an external table for NASDAQ daily prices data set.
-----------------
CREATE EXTERNAL TABLE nasdaq_dividend ( exchange1 STRING, stock_symbol STRING, modified_date STRING, dividends FLOAT ) COMMENT 'This is the nasdaq dividend table' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION '/input/nasdaq/dividends';




Question 2:Create an external table for NASDAQ dividends data set.
------------------
CREATE EXTERNAL TABLE nasdaq_prices (exchange1 STRING, stock_symbol STRING, modified_date STRING,stock_price_open FLOAT,stock_price_high FLOAT,stock_price_low FLOAT,stock_price_close FLOAT,stock_volume FLOAT,stock_price_adj_close FLOAT )
COMMENT 'This is the nasdaq prices table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/input/nasdaq/daily_price/';



Question 3: Find out total volume sale for each stock symbol which has closing price more than $5.
------------------
select stock_symbol, sum(stock_volume) as stock_volume from nasdaq_prices where stock_price_close > 5 group by stock_symbol;



Question 4:Find out highest price in the history for each stock symbol.
----------------
select stock_symbol, max(stock_price_close) as highest_price from nasdaq_prices group by stock_symbol;


Question 5: Find out highesh dividends given for each stock symbol in entire history.
--------------------

select stock_symbol, max(dividends) as highest_dividends from nasdaq_dividend group by stock_symbol;



Question 6:Find out highest price and highesh dividends for each stock symbol if highest price and highest dividends exist. 
---------------------
select np.stock_symbol, highest_price, highest_dividends from (select stock_symbol, max(stock_price_close) as highest_price from nasdaq_prices group by stock_symbol) as np join (select stock_symbol, max(dividends) as highest_dividends from nasdaq_dividend group by stock_symbol) as nd on np.stock_symbol = nd.stock_symbol;


Question 7: Find out highest price and highest dividends for each stock symbol, if one of them does not exist then keep Null values.
-------------------
select np.stock_symbol, highest_price, highest_dividends from (select stock_symbol, max(stock_price_close) as highest_price from nasdaq_prices group by stock_symbol) as np full outer join (select stock_symbol, max(dividends) as highest_dividends from nasdaq_dividend group by stock_symbol) as nd on np.stock_symbol = nd.stock_symbol;




