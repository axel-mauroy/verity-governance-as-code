-- models/staging/human_resources/stg_employees.sql
SELECT 
    employee_id,
    employee_name,
    department,
    role,
    CAST(hire_date AS DATE) as hire_date,
    salary_band
FROM {{ source('human_resources', 'employees') }}
