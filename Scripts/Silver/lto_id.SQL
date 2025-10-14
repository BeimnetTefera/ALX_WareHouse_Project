USE AlxDataWareHouse;
GO

TRUNCATE TABLE silver.unique_learners;

INSERT INTO silver.unique_learners (
    first_name,
    last_name,
    email,
    hubspot_id,
    contact_phone,
    age,
    age_range,
    country_of_residence,
    category_country_of_residence,
    city_of_residence,
    cohort_name,
    location_program_cohort,
    learner_hub_city,
    nile_cohort_name,
    first_check_in,
    most_recent_check_in,
    total_number_of_check_in_any_hub,
    no_of_check_in_most_visited_hub,
    current_month_to_day_check_in,
    check_ins_last_30_days,
    meets_attendance_criteria,
    most_visited_hub
)
SELECT DISTINCT
    first_name,
    last_name,
    email,
    hubspot_id,
    contact_phone,
    age,
    age_range,
    country_of_residence,
    category_country_of_residence,
    city_of_residence,
    cohort_name,
    location_program_cohort,
    learner_hub_city,
    nile_cohort_name,
    first_check_in,
    most_recent_check_in,
    total_number_of_check_in_any_hub,
    no_of_check_in_most_visited_hub,
    current_month_to_day_check_in,
    check_ins_last_30_days,
    meets_attendance_criteria,
    most_visited_hub
FROM bronze.pw_learner_tracking_overview;
GO
