## MySQL query optimization

### 1 point
### Task description

Optimize this query (optimized query should return the same result as the unoptimized query below):

~~~
use employees;

SELECT
    e.emp_no,
    e.first_name,
    e.last_name,

    /* current_department: compute current dept for ALL employees, then filter */
    (
        SELECT d.dept_name
        FROM departments d
                 JOIN (
            SELECT de_all.emp_no, de_all.dept_no
            FROM dept_emp de_all
                     JOIN (
                SELECT emp_no, MAX(from_date) AS max_from_date
                FROM dept_emp
                WHERE to_date = DATE '9999-01-01'
                GROUP BY emp_no
            ) mx
                          ON mx.emp_no = de_all.emp_no
                              AND mx.max_from_date = de_all.from_date
            WHERE de_all.to_date = DATE '9999-01-01'
            ORDER BY de_all.emp_no  /* useless sort */
        ) cur_dept_all
                      ON cur_dept_all.dept_no = d.dept_no
        WHERE cur_dept_all.emp_no = e.emp_no
        LIMIT 1
    ) AS current_department,

    /* current_title: compute current titles for ALL employees, then filter */
    (
        SELECT cur_title_all.title
        FROM (
            SELECT t_all.emp_no, t_all.title
            FROM titles t_all
            JOIN (
                SELECT emp_no, MAX(from_date) AS max_from_date
                FROM titles
                WHERE to_date = DATE '9999-01-01'
                GROUP BY emp_no
            ) mx
              ON mx.emp_no = t_all.emp_no
             AND mx.max_from_date = t_all.from_date
            WHERE t_all.to_date = DATE '9999-01-01'
            ORDER BY t_all.emp_no  /* useless sort */
        ) cur_title_all
        WHERE cur_title_all.emp_no = e.emp_no
        LIMIT 1
    ) AS current_title,

    e.hire_date,

    /* tenure: recompute, plus pointless nested selects */
    (SELECT DATEDIFF(CURRENT_DATE, (SELECT e2.hire_date FROM employees e2 WHERE e2.emp_no = e.emp_no))) AS days_working,
    (SELECT ROUND(DATEDIFF(CURRENT_DATE, (SELECT e3.hire_date FROM employees e3 WHERE e3.emp_no = e.emp_no)) / 365.25, 2)) AS years_working,

    /* current_salary: compute current salaries for ALL employees, then filter */
    (
        SELECT cur_sal_all.salary
        FROM (
            SELECT s_all.emp_no, s_all.salary
            FROM salaries s_all
            JOIN (
                SELECT emp_no, MAX(from_date) AS max_from_date
                FROM salaries
                WHERE to_date = DATE '9999-01-01'
                GROUP BY emp_no
            ) mx
              ON mx.emp_no = s_all.emp_no
             AND mx.max_from_date = s_all.from_date
            WHERE s_all.to_date = DATE '9999-01-01'
            ORDER BY s_all.salary DESC /* useless sort */
        ) cur_sal_all
        WHERE cur_sal_all.emp_no = e.emp_no
        LIMIT 1
    ) AS current_salary,

    /* amount_of_salary_raises: run window calc over ENTIRE salaries table */
    (
        SELECT SUM(
                   CASE
                       WHEN z.prev_salary IS NULL THEN 0
                       WHEN z.salary > z.prev_salary THEN 1
                       ELSE 0
                   END
               )
        FROM (
            SELECT
                s.emp_no,
                s.salary,
                LAG(s.salary) OVER (PARTITION BY s.emp_no ORDER BY s.from_date) AS prev_salary
            FROM salaries s
            /* no filter here: forces full-table window computation */
            ORDER BY s.emp_no, s.from_date /* useless sort on top */
        ) z
        WHERE z.emp_no = e.emp_no
    ) AS amount_of_salary_raises,

    /* sum_of_salary_raises: same full-table window calc AGAIN */
    (
        SELECT SUM(GREATEST(w.salary - COALESCE(w.prev_salary, w.salary), 0))
        FROM (
            SELECT
                s.emp_no,
                s.salary,
                LAG(s.salary) OVER (PARTITION BY s.emp_no ORDER BY s.from_date) AS prev_salary
            FROM salaries s
            /* no filter here: forces full-table window computation */
            ORDER BY s.emp_no, s.from_date
        ) w
        WHERE w.emp_no = e.emp_no
    ) AS sum_of_salary_raises,

    /* avg_salary_of_department: compute current dept for ALL, then avg current salary for ALL, then filter */
    (
        SELECT AVG(cur_sal.salary)
        FROM (
            SELECT de_all.emp_no, de_all.dept_no
            FROM dept_emp de_all
            WHERE de_all.to_date = DATE '9999-01-01'
            ORDER BY de_all.dept_no /* useless */
        ) cur_dept
        JOIN (
            SELECT s_all.emp_no, s_all.salary
            FROM salaries s_all
            WHERE s_all.to_date = DATE '9999-01-01'
            ORDER BY s_all.emp_no /* useless */
        ) cur_sal
          ON cur_sal.emp_no = cur_dept.emp_no
        WHERE cur_dept.dept_no = (
            SELECT de_emp.dept_no
            FROM dept_emp de_emp
            WHERE de_emp.emp_no = e.emp_no
              AND de_emp.to_date = DATE '9999-01-01'
            ORDER BY de_emp.from_date DESC
            LIMIT 1
        )
    ) AS avg_salary_of_department,

    /* delta_vs_dept_avg: recompute current_salary + dept_avg again */
    (
        (
            SELECT cur_sal_all.salary
            FROM (
                SELECT s_all.emp_no, s_all.salary
                FROM salaries s_all
                WHERE s_all.to_date = DATE '9999-01-01'
                ORDER BY s_all.salary DESC
            ) cur_sal_all
            WHERE cur_sal_all.emp_no = e.emp_no
            LIMIT 1
        )
        -
        (
            SELECT AVG(cur_sal.salary)
            FROM (
                SELECT de_all.emp_no, de_all.dept_no
                FROM dept_emp de_all
                WHERE de_all.to_date = DATE '9999-01-01'
                ORDER BY de_all.dept_no
            ) cur_dept
            JOIN (
                SELECT s_all.emp_no, s_all.salary
                FROM salaries s_all
                WHERE s_all.to_date = DATE '9999-01-01'
                ORDER BY s_all.emp_no
            ) cur_sal
              ON cur_sal.emp_no = cur_dept.emp_no
            WHERE cur_dept.dept_no = (
                SELECT de_emp.dept_no
                FROM dept_emp de_emp
                WHERE de_emp.emp_no = e.emp_no
                  AND de_emp.to_date = DATE '9999-01-01'
                ORDER BY de_emp.from_date DESC
                LIMIT 1
            )
        )
    ) AS delta_vs_dept_avg

FROM employees e
WHERE e.emp_no = 100001;

~~~

Query have to build a **single employee “current snapshot”** query (filtered by one `emp_no`) that returns:

* **Identity**: `emp_no`, `first_name`, `last_name`
* **Current org state** (only rows that are *current*):

    * `current_department` (from `dept_emp` → `departments`, where `dept_emp.to_date = '9999-01-01'`)
    * `current_title` (from `titles`, where `titles.to_date = '9999-01-01'`)
* **Tenure** (based on `employees.hire_date` and today):

    * `days_working` (difference in days)
    * `years_working` (days / 365.25, rounded)
* **Current compensation**:

    * `current_salary` (from `salaries`, where `salaries.to_date = '9999-01-01'`)
* **Salary growth history** (based on **all** rows in `salaries` for that employee, ordered by `from_date`):

    * `amount_of_salary_raises`: number of salary changes that are **increases** vs previous salary
    * `sum_of_salary_raises`: sum of all **positive** increases (ignore decreases/no-change)
* **Department benchmark (current)**:

    * `avg_salary_of_department`: average of `current_salary` for **all employees currently in the same department**
    * `delta_vs_dept_avg`: `current_salary - avg_salary_of_department`

### Sample output (result shape)

| emp_no | first_name | last_name      | current_department | current_title | hire_date  | days_working | years_working | current_salary | amount_of_salary_raises | sum_of_salary_raises | avg_salary_of_department | delta_vs_dept_avg |
| :----- | :--------- | :------------- | :----------------- | :------------ | :--------- | -----------: | ------------: | -------------: | ----------------------: | -------------------: | -----------------------: | ----------------: |
| 100001 | Jasminko   | Antonakopoulos | Research           | Engineer      | 1994-12-25 |        11360 |         31.10 |          42707 |                       3 |                 2707 |               67913.3750 |       -25206.3750 |

### Hints (MySQL functions, very short)

* `GREATEST(a, b, ...)`: returns the **largest** value.
* `COALESCE(a, b, ...)`: returns the **first non-NULL** value (useful for missing previous salary).
* `DATEDIFF(date1, date2)`: returns `date1 - date2` in **days** (use for `days_working`).
* `LAG(expr) OVER (PARTITION BY … ORDER BY …)`:
  Returns the value of `expr` from the **previous row** within the ordered partition
  → commonly used to compare the current row with the previous one (e.g., detect salary increases).
