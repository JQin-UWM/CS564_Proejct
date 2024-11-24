SELECT d.department_name, COUNT(e.department_id) AS employee_count 
FROM departments d 
LEFT JOIN employees e ON d.department_id = e.department_id 
GROUP BY e.department_id 
ORDER BY employee_count DESC;
