
show databases;
use employees;

# 1. Check data
show tables;
select count(*) from employees;
select * from employees limit 100;
show index from employees;

select count(*) from salaries;
select * from salaries;
show index from salaries;


# 2. Adding index demo
explain analyze select * from salaries where from_date = '2000-10-16';
/*
Before index output

-> Filter: (salaries.from_date = DATE'2000-10-16')  (cost=285373 rows=283843) (actual time=2.09..426 rows=667 loops=1)
    -> Table scan on salaries  (cost=285373 rows=2.84e+6) (actual time=2.06..358 rows=2.84e+6 loops=1)
 */

CREATE INDEX idx_salaries_from_date
    ON salaries (from_date);

explain analyze select * from salaries where from_date = '2000-10-16';

/*
With index
-> Index lookup on salaries using idx_salaries_from_date (from_date=DATE'2000-10-16')  (cost=324 rows=667) (actual time=12.2..14.2 rows=667 loops=1)
*/

# 3. Big table preparation (just to show the power of indexes).
-- drop table if exists employees_big_table;
CREATE TABLE employees_big_table AS
WITH RECURSIVE mult AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1
    FROM mult
    WHERE n < 100   -- number of copies
)
SELECT e.*
FROM employees e
         JOIN mult;


select count(*) from employees_big_table; -- 30 002 400
show indexes from employees_big_table;
select * from employees_big_table limit 100;

select * from employees_big_table limit 100 offset 2000000;

select max(emp_no) from employees_big_table;


explain analyze select * from employees_big_table where emp_no = 10946; -- 8 sec
/*
-> Filter: (employees_big_table.emp_no = 10946)  (cost=3.09e+6 rows=2.99e+6) (actual time=44.9..8530 rows=100 loops=1)
    -> Table scan on employees_big_table  (cost=3.09e+6 rows=29.9e+6) (actual time=0.168..7742 rows=30e+6 loops=1)
*/
CREATE INDEX idx_employees_big_table_emp_no
    ON employees_big_table (emp_no);

explain analyze select * from employees_big_table where emp_no = 10946; -- 30 ms
/*
-> Index lookup on employees_big_table using idx_employees_big_table_emp_no (emp_no=10946)  (cost=110 rows=100) (actual time=0.723..1.11 rows=100 loops=1)
*/
