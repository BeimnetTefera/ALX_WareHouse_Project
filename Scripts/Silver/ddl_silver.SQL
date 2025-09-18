/*
================================================================================
DDL Script: Create Silver Tables
================================================================================
Script Purpose:
    This script creates tables in the 'Silver' schema, dropping existing tables
    if they already exixst
Run this script to re-define the DDL structure of 'Silver' tables
*/

USE AlxDataWareHouse;
GO

-- Drop Silver tables if they exist
IF OBJECT_ID('silver.pw_learner_activity', 'U') IS NOT NULL
    DROP TABLE silver.pw_learner_activity;
GO

IF OBJECT_ID('silver.pw_learner_tracking_overview', 'U') IS NOT NULL
    DROP TABLE silver.pw_learner_tracking_overview;
GO

-- Create pw_learner_activity (Silver)
CREATE TABLE silver.pw_learner_activity(
    first_name NVARCHAR(100),
    last_name NVARCHAR(100),
    email NVARCHAR(100),
    course_name NVARCHAR(150),
    assignment_name NVARCHAR(150),
    is_assignment_accessed NVARCHAR(5),
    is_assignment_submitted NVARCHAR(5),
    is_assignment_passed NVARCHAR(5),
    contact_phone NVARCHAR(20),
    age INT,
    age_range NVARCHAR(15),
    country_of_residence NVARCHAR(50),
    category_country_of_residence NVARCHAR(50),
    city_of_residence NVARCHAR(50),
    day_of_activation INT,
    cohort_name NVARCHAR(20),
    class_name NVARCHAR(20),
    location_program_cohort NVARCHAR(50),
    assignment_type NVARCHAR(20),
    assignment_score FLOAT,
    learner_enrolled_into_lms NVARCHAR(5),
    has_logged_into_lms NVARCHAR(5),
    has_logged_into_eHub NVARCHAR(5),
    has_shown_up_in_circle NVARCHAR(5),
    has_shown_up_in_circle_during_the_last_two_days NVARCHAR(5),
    is_engaged_in_circle NVARCHAR(5),
    is_engaged_in_circle_during_the_last_two_days NVARCHAR(5),
    enrollment_program_activated NVARCHAR(5),
    enrollment_in_previous_withdrawn_class NVARCHAR(5),
    enrollment_course_activated NVARCHAR(5),
    is_program_withdrawn NVARCHAR(5),
    program_withdrawal_reason NVARCHAR(254),
    program_withdrawn_date DATETIME,
    is_course_deferred NVARCHAR(5),
    course_deferred_date DATETIME,
    on_track_for_on_time_graduation NVARCHAR(5),
    on_track_for_delayed_graduation NVARCHAR(5),
    is_assignment_resubmitted NVARCHAR(5),
    is_assignment_resubmitted_and_not_corrected_yet NVARCHAR(5),
    is_assignment_graded NVARCHAR(5),
    is_assignment_corrected_by_an_external_grader NVARCHAR(50),
    corrected_external_grader_name NVARCHAR(50),
    corrected_external_grader_email NVARCHAR(100),
    is_on_track NVARCHAR(5),
    no_of_assignment_submissions INT,
    submission_link NVARCHAR(250),
    no_of_assignmnets INT,
    no_of_submissions INT,
    no_of_assignments_passed INT,
    no_of_milestone INT,
    no_of_milestone_submissions INT,
    no_of_milestones_passed INT,
    no_of_quizzes INT,
    no_of_quizzes_submitted INT,
    no_of_quizzes_passed INT,
    no_of_tests INT,
    no_of_tests_submissions INT,
    no_of_tests_passed INT,
    lms_overall_score FLOAT,
    is_lms_overall_score_higher_than_score_threshold NVARCHAR(5),
    lms_course_status NVARCHAR(15),
    ehub_course_status NVARCHAR(10),
    ehub_class_status NVARCHAR(10),
    is_course_paused NVARCHAR(5),
    course_leave_of_absence_reason NVARCHAR(250)
);
GO

-- Create pw_learner_tracking_overview (Silver)
CREATE TABLE silver.pw_learner_tracking_overview(
    first_name NVARCHAR(100),
    last_name NVARCHAR(100),
    email NVARCHAR(100),
    course_name NVARCHAR(150),
    no_of_submissions INT,
    no_of_assignments_passed INT,
    lms_overall_score FLOAT,
    hubspot_id NVARCHAR(50),
    contact_phone NVARCHAR(20),
    age INT,
    age_range NVARCHAR(15),
    country_of_residence NVARCHAR(50),
    category_country_of_residence NVARCHAR(50),
    city_of_residence NVARCHAR(50),
    cohort_name NVARCHAR(20),
    location_program_cohort NVARCHAR(50),
    learner_enrolled_into_lms NVARCHAR(5),
    has_logged_into_lms NVARCHAR(5),
    has_logged_into_eHub NVARCHAR(5),
    has_shown_up_in_circle NVARCHAR(5),
    has_shown_up_in_circle_during_the_last_two_days NVARCHAR(5),
    is_engaged_in_circle NVARCHAR(5),
    is_engaged_in_circle_during_the_last_two_days NVARCHAR(5),
    enrollment_program_activated NVARCHAR(5),
    class_name NVARCHAR(20),
    enrollment_course_activated NVARCHAR(5),
    is_program_withdrawn NVARCHAR(5),
    program_withdrawal_reason NVARCHAR(254),
    program_withdrawn_date DATETIME,
    is_course_deferred NVARCHAR(5),
    course_deferred_date DATETIME,
    on_track_for_on_time_graduation NVARCHAR(5),
    on_track_for_delayed_graduation NVARCHAR(5),
    no_of_assignments INT,
    no_of_milestone INT,
    no_of_milestone_submissions INT,
    no_of_milestone_passed INT,
    no_of_practice_quiz INT,
    no_of_practice_quiz_submissions INT,
    no_of_practice_quiz_passed INT,
    no_of_test INT,
    no_of_test_submissions INT,
    no_of_test_passed INT,
    assignments_accessed NVARCHAR(250),
    assignments_submitted NVARCHAR(250),
    assignments_missing NVARCHAR(250),
    assignments_passed NVARCHAR(250),
    assignments_submitted_and_failed NVARCHAR(250),  -- fixed mismatch
    learner_hub_city NVARCHAR(50),
    first_check_in DATETIME,
    most_recent_check_in DATETIME,
    total_number_of_check_in_any_hub INT,
    no_of_check_in_most_visited_hub INT,
    current_month_to_day_check_in INT,
    check_ins_last_30_days INT,
    meets_attendance_criteria NVARCHAR(5),
    most_visited_hub NVARCHAR(50),
    is_lms_overall_score_higher_than_threshold NVARCHAR(5),
    lms_course_status NVARCHAR(15),
    ehub_course_status NVARCHAR(10),
    ehub_class_status NVARCHAR(10),
    is_course_paused NVARCHAR(5),
    course_leave_of_absence_reason NVARCHAR(250),
    nile_cohort_name NVARCHAR(50)
);
