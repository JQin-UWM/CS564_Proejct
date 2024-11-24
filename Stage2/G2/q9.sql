SELECT d.department_name
FROM departments d
JOIN employees e ON d.department_id = e.department_id
ORDER BY e.salary DESC
LIMIT 1;
