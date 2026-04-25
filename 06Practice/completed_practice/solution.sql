USE employees;

SHOW TABLES;

CREATE OR REPLACE VIEW employees_data AS
SELECT e.emp_no, e.birth_date, e.first_name, e.last_name, e.gender, e.hire_date, de.dept_no, d.dept_name, s.salary, s.from_date, s.to_date
FROM
    employees e
    JOIN dept_emp de ON e.emp_no = de.emp_no
    JOIN departments d ON de.dept_no = d.dept_no
    JOIN salaries s ON e.emp_no = s.emp_no;