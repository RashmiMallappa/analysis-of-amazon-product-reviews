------------Query to create a table-------------------
CREATE EXTERNAL TABLE IF NOT EXISTS amazondata.books_review (
  `product_id` int,
  `reviwer_id` string,
  `reviwer_name` string,
  `rating` int,
  `unixreviewtime` bigint,
  `review` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://projectamazondata/booksreview/'
TBLPROPERTIES ('has_encrypted_data'='false');

------------------number of reviews given by user-----------------------------
SELECT reviwer_id,count(reviwer_id) AS numberofreviews
FROM "amazondata"."book_review"  
group by reviwer_id
order by numberofreviews;
-----------------Create view-------------------------------------------------
CREATE OR REPLACE VIEW NumberofReviews AS
SELECT reviwer_id,count(reviwer_id) AS reviewcount
FROM "amazondata"."book_review"  
group by reviwer_id
order by reviewcount;

SELECT reviewcount ,count(reviewcount) As totalcount
FROM "amazondata"."numberofreviews" 
where reviewcount > 0
group by reviewcount
order by reviewcount ;

----------------number of reviews per product---------------------------------
SELECT concat(product_id,'p'),count(product_id) AS numberofreviews
FROM "amazondata"."book_review"  
group by product_id
order by numberofreviews
limit 200;

-----------------Create view---------------------------------------------------
CREATE OR REPLACE VIEW nuberofreviews_rproduct AS
SELECT concat(product_id,'p') As product_id,count(product_id) AS numberofreviews
FROM "amazondata"."book_review"  
group by product_id
order by numberofreviews;

SELECT numberofreviews ,count(numberofreviews) As numberofproducts
FROM "amazondata"."numberofreviews_product" 
where numberofreviews > 0
group by numberofreviews
order by numberofreviews ;

-----------------------Average rating of products--------------------------------
SELECT product_id as book_id ,avg(rating) As averagerating
FROM "amazondata"."books_review" 
where rating <= 5.0
group by product_id
order by averagerating desc ;

-----------------------Create view-----------------------------------------------
CREATE OR REPLACE VIEW product_avgrating AS
SELECT product_id as book_id ,avg(rating) As averagerating
FROM "amazondata"."books_review" 
where rating <= 5.0
group by product_id
order by averagerating desc ;

SELECT ROUND(averagerating,2) As bookrating ,count(averagerating) As numberofbooks
FROM "amazondata"."product_avgrating" 
where averagerating <= 5
group by averagerating
order by numberofbooks desc ;

-----------------------Numberofproducts and ratings----------------------------------
SELECT rating as rating ,count(product_id) As totalcount
FROM "amazondata"."books_review" 
where rating <= 5.0
group by rating
order by totalcount desc ;

-----------------------Numberofusers and ratings---------------------------------------
SELECT rating as rating ,count(reviwer_id) As totalcount
FROM "amazondata"."books_review" 
where rating <= 5.0
group by rating
order by totalcount desc ;




