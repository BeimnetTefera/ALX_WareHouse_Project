SELECT
    TRIM(
        CASE 
            WHEN contact_phone LIKE '+251%' THEN contact_phone
            WHEN contact_phone LIKE '251%'  THEN CONCAT('+', contact_phone)
            WHEN contact_phone LIKE '0%'    THEN CONCAT('+251', SUBSTRING(contact_phone, 2, LEN(contact_phone)))
            ELSE CONCAT('+251', contact_phone)
        END
    ) AS clean_phone
FROM bronze.pw_learner_activity;
