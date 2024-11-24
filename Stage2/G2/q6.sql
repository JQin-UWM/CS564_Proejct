SELECT COUNT(e.employee_id)
FROM employees e, departments d, locations l, countries c, regions r
WHERE d.department_id = e.department_id AND d.location_id = l.location_id AND l.country_id = c.country_id AND c.region_id = r.region_id AND r.region_name = 'Europe';
