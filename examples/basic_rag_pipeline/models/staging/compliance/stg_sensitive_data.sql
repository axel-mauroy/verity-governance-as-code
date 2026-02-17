-- models/staging/compliance/stg_sensitive_data.sql
-- ⚠️ GOVERNANCE TEST: This model SHOULD BE BLOCKED by Verity
-- because it contains SSN and Credit Card data with action: block

SELECT 
    record_id,
    customer_name,
    email,        -- PII: High → Mask
    phone,        -- PII: High → Mask
    ssn,          -- PII: Critical → BLOCK
    credit_card,  -- PII: Critical → BLOCK
    notes
FROM {{ source('compliance', 'sensitive_data') }}
