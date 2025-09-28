USE AlxDataWareHouse;
GO

SELECT
    first_name AS First_Nmae,
    last_name,
    email,
    course_name,
    lms_overall_score,
    CASE 
        WHEN lms_overall_score >= 70 THEN 'YES'
        ELSE 'NO'
    END AS is_lms_overall_score_higher_than_threshold,
    CASE 
        WHEN lms_overall_score >= 70 THEN 'YES'
        ELSE 'NO'
    END AS is_on_track
    
FROM silver.pw_learner_activity;
