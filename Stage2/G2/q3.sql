SELECT d.department_name, AVG(j.max_salary) AS avg_max_salary
FROM departments d, employees e, jobs j
WHERE d.department_id = e.department_id
AND e.job_id = j.job_id
GROUP BY d.department_id
HAVING AVG(j.max_salary) > 8000;
