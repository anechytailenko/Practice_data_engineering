DROP VIEW IF EXISTS v_dept_benchmark;

-- What is the average salary we are paying right now in each department?
CREATE VIEW v_dept_benchmark AS
SELECT de.dept_no, AVG(s.salary) AS avg_dept_salary
FROM dept_emp de
    JOIN salaries s ON de.emp_no = s.emp_no
    AND s.to_date = '9999-01-01'
WHERE
    de.to_date = '9999-01-01'
GROUP BY
    de.dept_no;

DROP VIEW IF EXISTS v_emp_salary_history;

-- Career Growth Scorecard for each employee: how many times did they get a raise and what is the total amount of raises they got?
CREATE VIEW v_emp_salary_history AS
SELECT
    emp_no,
    SUM(
        CASE
            WHEN salary > prev_salary THEN 1
            ELSE 0
        END
    ) AS amount_of_salary_raises,
    SUM(
        GREATEST(
            salary - COALESCE(prev_salary, salary),
            0
        )
    ) AS sum_of_salary_raises
FROM (
        SELECT emp_no, salary, LAG(salary) OVER (
                PARTITION BY
                    emp_no
                ORDER BY from_date
            ) AS prev_salary
        FROM salaries
    ) window_calc
GROUP BY
    emp_no;

DROP VIEW IF EXISTS v_emp_snapshot;

-- snapshot for each employee, that comine full characteristic of employee with current salary stats across departments and career growth scorecard.
CREATE VIEW v_emp_snapshot AS
SELECT
    e.emp_no,
    e.first_name,
    e.last_name,
    d.dept_name AS current_department,
    t.title AS current_title,
    e.hire_date,
    DATEDIFF(CURDATE(), e.hire_date) AS days_working,
    ROUND(
        DATEDIFF(CURDATE(), e.hire_date) / 365.25,
        2
    ) AS years_working,
    s.salary AS current_salary,
    COALESCE(h.amount_of_salary_raises, 0) AS amount_of_salary_raises,
    COALESCE(h.sum_of_salary_raises, 0) AS sum_of_salary_raises,
    b.avg_dept_salary AS avg_salary_of_department,
    (s.salary - b.avg_dept_salary) AS delta_vs_dept_avg
FROM
    employees e
    JOIN dept_emp de ON e.emp_no = de.emp_no
    AND de.to_date = '9999-01-01'
    JOIN departments d ON de.dept_no = d.dept_no
    JOIN titles t ON e.emp_no = t.emp_no
    AND t.to_date = '9999-01-01'
    JOIN salaries s ON e.emp_no = s.emp_no
    AND s.to_date = '9999-01-01'
    LEFT JOIN v_emp_salary_history h ON e.emp_no = h.emp_no
    LEFT JOIN v_dept_benchmark b ON de.dept_no = b.dept_no;

SELECT * FROM v_emp_snapshot WHERE emp_no = 100001 LIMIT 1;