### Task: Top Discounts per Product
## 1 Point

Write a DuckDB SQL query that:

* Reads product data from a [Nike Dataset JSON file](https://www.kaggle.com/datasets/matepapava/nike-discounts-dataset)
* Selects product price and discount fields
* Uses a **window function** to rank rows by `discount_percent` (highest first, then lowest `current_price`)
* Keeps only the **top 10 rows per product title**
* Returns the results sorted by `product_type`, `title`, and rank

**Constraints**

* Use a CTE
* Use `ROW_NUMBER()`
* Use Window Function

**Result Set Example**

| product\_type | title | discount\_percent | current\_price | original\_price |
| :--- | :--- | :--- | :--- | :--- |
| discount | Air Jordan | 13 | 25.97 | 30 |
| discount | Air Jordan | 23 | 26.97 | 35 |
| discount | Air Jordan | 29 | 38.97 | 55 |
| discount | Air Jordan 1 Low | 14 | 102.97 | 120 |
| discount | Air Jordan 1 Low | 14 | 102.97 | 120 |
| discount | Air Jordan 1 Low | 14 | 102.97 | 120 |
| discount | Air Jordan 1 Low | 14 | 102.97 | 120 |
| discount | Air Jordan 1 Low | 14 | 76.97 | 90 |
