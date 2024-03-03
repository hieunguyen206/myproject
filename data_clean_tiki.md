**Source of the dataset:** https://www.kaggle.com/datasets/biminhc/tiki-books-dataset
**link Big querry:** https://console.cloud.google.com/bigquery?project=hieu-la-bi&ws=!1m4!1m3!3m2!1shieu-la-bi!2skaggle_cleansed&authuser=1

WITH rename_column AS (
SELECT 
  product_id AS tiki_product_id
  ,title
  ,authors
  ,original_price
  ,current_price AS discount_price
  ,quantity AS sold_quantity
  ,category
  ,n_review AS count_review
  ,avg_rating
  ,pages AS book_pages
  ,manufacturer AS publisher
  ,cover_link
FROM `vit-lam-data.kaggle_tiki_books.book_data` 
)
, cast_type AS (
SELECT *
  EXCEPT(avg_rating, book_pages)
  , CAST(avg_rating AS NUMERIC) AS avg_rating
  , CAST(NULLIF(book_pages, 'Cuốn') AS INTEGER) AS book_pages
FROM rename_column
)
, deduplicate AS (
SELECT DISTINCT *
FROM cast_type
)
, remove_wrong_data AS (
SELECT *
FROM deduplicate
WHERE sold_quantity IS NOT NULL
)
, handle_null AS (
SELECT 
tiki_product_id
  ,title
  ,COALESCE(NULLIF(authors, '.'),'Unidentify') AS authors 
  ,original_price
  ,discount_price
  ,sold_quantity
  ,category
  ,count_review
  ,NULLIF(avg_rating, 0) AS avg_rating
  ,book_pages
  ,publisher
  ,cover_link
FROM remove_wrong_data
)
, clean_value AS (
SELECT *
  EXCEPT(category)
  , CASE 
  WHEN category IN (
    SELECT category
    FROM handle_null
    GROUP BY 1
    HAVING COUNT(*) =1) THEN 'Unidentify'
    ELSE category
    END AS category
  , REPLACE(publisher, 'Nhà xuất bản', 'NXB') AS publisher
FROM handle_null
)

SELECT *
FROM clean_value
ORDER BY tiki_product_id
