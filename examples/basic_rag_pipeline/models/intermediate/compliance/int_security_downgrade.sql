-- models/intermediate/compliance/int_security_downgrade.sql
-- ⚠️ GOVERNANCE TEST: This model SHOULD FAIL
-- because it downgrades from Confidential to Public

/* 
config:
  name: int_security_downgrade
  materialized: view
  governance:
    public: true
    pii: false
    security: public
*/

-- Attempting to read Confidential data into a Public model
SELECT 
    record_id,
    customer_name,
    notes
FROM {{ ref('stg_sensitive_data') }}
