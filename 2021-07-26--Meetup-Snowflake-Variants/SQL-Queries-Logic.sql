---------------------------------------------------
-- AICG 
-- Snowflake Carolinas Meetup
-- 20210726
-- TOPIC: Snowflake Variants
---------------------------------------------------

-- change role to securityadmin for user / role steps
use role datalakehouse_role;
use database demo_db;
use warehouse datalakehouse_wh;

-- create schema for the meetup
create or replace schema meetup_db_20210726 comment='Snowflake Meetup - 20210726';

-- create basic table with single column with variant data type 
create or replace table meetup_variants comment='Variant data type discussion' (
    json_data variant
)
;

-- could use the COPY INTO statement with SNOW CLI or simply....

-- insert into the table the single column with parse_json
insert into meetup_variants 
select parse_json('
{
    "12 Strong": {
        "Genre": "Action",
        "Gross": "$1,465,000",
        "IMDB Metascore": "54",
        "Popcorn Score": 72,
        "Rating": "R",
        "Tomato Score": 54
    },
    "A Ciambra": {
        "Genre": "Drama",
        "Gross": "unknown",
        "IMDB Metascore": "70",
        "Popcorn Score": "unknown",
        "Rating": "unrated",
        "Tomato Score": "unkown"
    },
    "All The Money In The World": {
        "popcornscore": 72,
        "rating": "R",
        "tomatoscore": 76
    },
    "Along With The Gods: The Two Worlds": {
        "popcornscore": 90,
        "rating": "NR",
        "tomatoscore": 50
    },
    "Bilal: A New Breed Of Hero": {
        "Genre": "Animation",
        "Gross": "unknown",
        "IMDB Metascore": "52",
        "Popcorn Score": "unknown",
        "Rating": "unrated",
        "Tomato Score": "unkown"
    },
    "Call Me By Your Name": {
        "popcornscore": 87,
        "rating": "R",
        "tomatoscore": 96
    },
    "Condorito: La Pel\u00edcula": {
        "popcornscore": 59,
        "rating": "PG",
        "tomatoscore": 40
    },
    "Darkest Hour": {
        "popcornscore": 84,
        "rating": "PG13",
        "tomatoscore": 86
    },
    "Den Of Thieves": {
        "Genre": "Action",
        "Gross": "$1,430,000",
        "IMDB Metascore": "50",
        "Popcorn Score": 71,
        "Rating": "R",
        "Tomato Score": 40
    },
    "Downsizing": {
        "popcornscore": 24,
        "rating": "R",
        "tomatoscore": 51
    },
    "Father Figures": {
        "popcornscore": 38,
        "rating": "R",
        "tomatoscore": 26
    },
    "Film Stars Don\'t Die In Liverpool": {
        "popcornscore": 70,
        "rating": "R",
        "tomatoscore": 78
    },
    "Forever My Girl": {
        "Genre": "Drama",
        "Gross": "$730,000",
        "IMDB Metascore": "36",
        "Popcorn Score": 91,
        "Rating": "PG",
        "Tomato Score": 21
    },
    "Happy End": {
        "popcornscore": 60,
        "rating": "R",
        "tomatoscore": 69
    },
    "Hostiles": {
        "popcornscore": 71,
        "rating": "R",
        "tomatoscore": 72
    },
    "I, Tonya": {
        "popcornscore": 89,
        "rating": "R",
        "tomatoscore": 90
    },
    "In The Fade (Aus Dem Nichts)": {
        "popcornscore": 60,
        "rating": "R",
        "tomatoscore": 72
    },
    "Insidious: The Last Key": {
        "Genre": "Horror",
        "Gross": "$456,000",
        "IMDB Metascore": "49",
        "Popcorn Score": 51,
        "Rating": "PG13",
        "Tomato Score": 31
    },
    "Jumanji: Welcome To The Jungle": {
        "Genre": "Action",
        "Gross": "$2,865,000",
        "IMDB Metascore": "58",
        "Popcorn Score": 90,
        "Rating": "PG13",
        "Tomato Score": 76
    },
    "Mary And The Witch\'s Flower": {
        "popcornscore": 77,
        "rating": "PG",
        "tomatoscore": 84
    },
    "Maze Runner: The Death Cure": {
        "popcornscore": 74,
        "rating": "PG13",
        "tomatoscore": 43
    },
    "Molly\'s Game": {
        "popcornscore": 86,
        "rating": "R",
        "tomatoscore": 81
    },
    "Paddington 2": {
        "Genre": "Animation",
        "Gross": "$660,000",
        "IMDB Metascore": "88",
        "Popcorn Score": 90,
        "Rating": "PG",
        "Tomato Score": 100
    },
    "Padmaavat": {
        "popcornscore": 63,
        "rating": "NR",
        "tomatoscore": 74
    },
    "Phantom Thread": {
        "popcornscore": 69,
        "rating": "R",
        "tomatoscore": 91
    },
    "Pitch Perfect 3": {
        "popcornscore": 52,
        "rating": "PG13",
        "tomatoscore": 31
    },
    "Proud Mary": {
        "popcornscore": 56,
        "rating": "R",
        "tomatoscore": 26
    },
    "Star Wars: Episode Viii - The Last Jedi": {
        "Genre": "Action",
        "Gross": "unknown",
        "IMDB Metascore": "85",
        "Popcorn Score": "unknown",
        "Rating": "unrated",
        "Tomato Score": "unkown"
    },
    "Star Wars: The Last Jedi": {
        "popcornscore": 48,
        "rating": "PG13",
        "tomatoscore": 91
    },
    "The Cage Fighter": {
        "Genre": "Documentary",
        "Gross": "unknown",
        "IMDB Metascore": "74",
        "Popcorn Score": "unknown",
        "Rating": "unrated",
        "Tomato Score": "unkown"
    },
    "The Commuter": {
        "Genre": "Action",
        "Gross": "$530,000",
        "IMDB Metascore": "56",
        "Popcorn Score": 49,
        "Rating": "PG13",
        "Tomato Score": 58
    },
    "The Final Year": {
        "popcornscore": 48,
        "rating": "NR",
        "tomatoscore": 84
    },
    "The Greatest Showman": {
        "Genre": "Biography",
        "Gross": "$2,275,000",
        "IMDB Metascore": "48",
        "Popcorn Score": 90,
        "Rating": "PG",
        "Tomato Score": 55
    },
    "The Insult": {
        "popcornscore": 90,
        "rating": "R",
        "tomatoscore": 88
    },
    "The Post": {
        "Genre": "Biography",
        "Gross": "$1,570,000",
        "IMDB Metascore": "83",
        "Popcorn Score": 73,
        "Rating": "PG13",
        "Tomato Score": 88
    },
    "The Shape Of Water": {
        "popcornscore": 79,
        "rating": "R",
        "tomatoscore": 92
    },
    "Una Mujer Fant\u00e1stica": {
        "Genre": "Drama",
        "Gross": "unknown",
        "IMDB Metascore": "89",
        "Popcorn Score": "unknown",
        "Rating": "unrated",
        "Tomato Score": "unkown"
    },
    "Winchester": {
        "Genre": "Biography",
        "Gross": "$3,606,000",
        "IMDB Metascore": "31",
        "Popcorn Score": "unknown",
        "Rating": "unrated",
        "Tomato Score": "unkown"
    }
  }                     
'
)
;


create or replace table car_sales
(
  src variant
)
as
select parse_json(column1) as src
from values
('{
    "date" : "2017-04-28",
    "dealership" : "Valley View Auto Sales",
    "salesperson" : {
      "id": "55",
      "name": "Frank Beasley"
    },
    "customer" : [
      {"name": "Joyce Ridgely", "phone": "16504378889", "address": "San Francisco, CA"}
      ,{"name": "Samantha Bradley", "phone": "18180987654", "address": "Irving, CA"}
      ,{"name": "Leonard Indianland", "phone": "16509876549", "address": "San Diego, CA"}
    ],
    "vehicle" : [
      {"make": "Honda", "model": "Civic", "year": "2017", "price": "20275", "extras":["ext warranty", "paint protection"]}
    ]
}')
,
('{
    "date" : "2017-04-28",
    "dealership" : "Tindel Toyota",
    "salesperson" : {
      "id": "274",
      "name": "Greg Northrup"
    },
    "customer" : [
      {"name": "Bradley Greenbloom", "phone": "12127593751", "address": "New York, NY"}
     ,{"name": "Sally Field", "phone": "12121234566", "address": "Upsate, NY"}
     ,{"name": "Lee Iacola", "phone": "12120987646", "address": "Manacasca, NY"}
    ],
    "vehicle" : [
      {"make": "Toyota", "model": "Camry", "year": "2017", "price": "23500", "extras":["ext warranty", "rust proofing", "fabric protection"]}
    ]
}') 

v;


-- check that data is available
select * from meetup_variants;
select * from car_sales;


-- lateral joins (to avoid subqueries)
select json_data:"12 Strong" from meetup_variants;
select json_data[0] from meetup_variants;

select json_data from meetup_variants 
, lateral flatten(input => (meetup_variants.json_data));

select 
  key                  as "_key"
  ,
  VALUE:Genre::string    as "Genre",
  VALUE:Rating::string   as "Rating"
from 
    meetup_variants
    , lateral flatten(input => (meetup_variants.json_data))
;



-- verify the key attributes:
/**
customer > ARRAY of object (address, name, phone)
date
dealership
salesperson > (id, name)
vehicle ARRAY of Object (extras Array, make, model, price year)
*/

-- get list of items ( two columns in this scenario instead of all JSON in a single column single row)
select src:dealership from car_sales;


-- let's use dot notation (be careful these are case sensitive)
select src:customer.address from car_sales; -- doesn't work because it is an array of objects
select src:customer from car_sales; -- see the array
select src:customer[0] from car_sales; -- get the first item in the array

select src:customer[0].name from car_sales; -- get the first item in the array


-- get the name attribute of a salesperson
select src:salesperson.name, src:customer[0].name as Customer from car_sales;
-- or, bracket notation
select src['salesperson']['name'] from car_sales;

-- CAST a value to explicity string (remember any retrieve on variant without cast returns a VARIANT string literal)
select src:salesperson.name::string from car_sales;



-- Now use the FLATTEN function to parse arrays 
-- getting a row for each object = FLATTEN
-- joining variant data with other information
-- it uses the special value prefix identifider
select
  value:name::string as "Customer Name",
  value:address::string as "Address"
  from
    car_sales
  , lateral flatten(input => src:customer);
  

-- flatten the internal array of the initial flattened retrieval
-- the query can take some additional time depending on nesting, compute, etc.
select
  vm.value:make::string as make,
  vm.value:model::string as model,
  ve.value::string as "Extras Purchased"
  from
    car_sales
  , lateral flatten(input => src:vehicle) vm
  , lateral flatten(input => vm.value:extras) ve;


-- using GET_PATH or GET to get a specific variant path
select get_path(src, 'vehicle[0]:make') from car_sales;


-- Advanced :   Use Case = Get List of Distinct Key Names in a Semi-structured JSON data
--              What if there is a list of key/value pairs where you need to get only the key names as a 
--              DISTINCT list for some other purpose of analysis
select 
//    DISTINCT(regexp_replace(f.path, '\\\\[[0-9]+\\\\]', '[]')) as "Path"
    DISTINCT(regexp_replace(f.path, '\\\\[[0-9]+\\\\]', '[]')) as "Path"
    , typeof(f.value) as "Type"
    , count(*) as "Count"
from car_sales, 
        lateral flatten(src, recursive => false) f
group by 1, 2 order by 1, 2;


-- Advanced:    Use Case, turning relational data into a well-formed JSON document
--              by parsing an existing SQL table using OBJECT_CONSTRUCT() syntax
--              Thanks to M. Rainey, https://community.snowflake.com/s/article/Generating-a-JSON-Dataset-using-Relational-Data-in-Snowflake
SELECT * FROM     "DLH_DEMO_SANDBOX"."DLH_PREFIX_SALES"."SALESORDERHEADER" A;

with 
sold_products as 
(
  select salesorderid,
  array_agg(object_construct(
    'product_id', productid
  ))
  
  from
    "DLH_DEMO_SANDBOX"."DLH_PREFIX_SALES"."SALESORDERDETAIL" a 
  group by salesorderid
)
,
sold_reason as 
(
  select salesorderid,
  array_agg(object_construct(
    'sales_reason_id', salesreasonid
  )) sales_reasons
  from
    "DLH_DEMO_SANDBOX"."DLH_PREFIX_SALES"."SALESORDERHEADERSALESREASON" a 
  group by salesorderid
)
select object_construct (
    '_salesorderid', a.salesorderid
  , 'sales_order_num', a.salesordernumber
  , 'order_date', a.orderdate
  , 'due_date', a.duedate
  , 'status', a.status
  , 'is_online_order', a.onlineorderflag
  , 'sales_reasons', sold_reason.sales_reasons
)
from
    "DLH_DEMO_SANDBOX"."DLH_PREFIX_SALES"."SALESORDERHEADER" A 
    inner join sold_reason on a.salesorderid = sold_reason.salesorderid
;


-- Advanced:    Filtering on JSON object attributes
select 
  key                  as "_key"
  ,
  VALUE:Genre::string    as "Genre",
  VALUE:Rating::string   as "Rating"
from 
    meetup_variants
    , lateral flatten(input => (meetup_variants.json_data))
where
    VALUE:Rating::string = 'R'
;
