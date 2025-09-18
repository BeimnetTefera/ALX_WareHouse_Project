/*
================================================================================
DDL Script: Create Bronze Table
================================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables
    if they already exixst
Run this script to re-define the DDL structure of 'bronze' tables
*/

USE AlxDataWareHouse;
GO
----------------------------------------------------------
-- Drop Table 'bronze.pw_learner_activity' if exists
----------------------------------------------------------
IF OBJECT_ID('bronze.pw_learner_activity', 'U') IS NOT NULL
    DROP TABLE bronze.pw_learner_activity;

CREATE TABLE bronze.pw_learner_activity(
    first_name NVARCHAR(50),
    last_name NVARCHAR(50),
    email NVARCHAR(100),
    course_name NVARCHAR(150),
    assignment_name NVARCHAR(150),
    is_assignment_accessed NVARCHAR(20),
    is_assignment_submitted NVARCHAR(20),
    is_assignment_passed NVARCHAR(20),
    contact_phone NVARCHAR(20),
    age NVARCHAR(10),
    age_range NVARCHAR(15),
    country_of_residence NVARCHAR(50),
    category_country_of_residences NVARCHAR(50),
    city_of_residence NVARCHAR(50),
    day_of_activation NVARCHAR(10),
    cohort_name NVARCHAR(50),
    class_name NVARCHAR(50),
    location_program_cohort NVARCHAR(50),
    assignment_type NVARCHAR(50),
    assignment_score NVARCHAR(20),
    learner_enrolled_into_lms NVARCHAR(20),
    has_logged_into_lms NVARCHAR(20),
    has_logged_into_eHub NVARCHAR(20),
    has_shown_up_in_circle NVARCHAR(20),
    has_shown_up_in_circle_during_the_last_two_days NVARCHAR(20),
    is_engaged_in_circle NVARCHAR(20),
    is_engaged_in_circle_during_the_last_two_days NVARCHAR(20),
    enrollment_program_activated NVARCHAR(20),
    enrollment_in_previous_withdrawn_class NVARCHAR(20),
    enrollment_course_activated NVARCHAR(20),
    is_program_withdrawn NVARCHAR(20),
    program_withdrawal_reason NVARCHAR(254),
    program_withdrawn_date NVARCHAR(50),
    is_course_deferred NVARCHAR(20),
    course_deferred_date NVARCHAR(50),
    on_track_for_on_time_graduation NVARCHAR(20),
    on_track_for_delayed_graduation NVARCHAR(20),
    is_assignment_resubmitted NVARCHAR(20),
    is_assignment_resubmitted_and_not_corrected_yet NVARCHAR(20),
    is_assignment_graded NVARCHAR(20),
    is_assignment_corrected_by_an_external_grader NVARCHAR(20),
    corrected_external_grader_name NVARCHAR(50),
    corrected_external_grader_email NVARCHAR(100),
    is_on_track NVARCHAR(20),
    no_of_assignment_submissions NVARCHAR(20),
    submission_link NVARCHAR(250),
    no_of_assignmnets NVARCHAR(20),
    no_of_submissions NVARCHAR(20),
    no_assignments_passed NVARCHAR(20),
    no_of_milestone NVARCHAR(20),
    no_of_milestone_submissions NVARCHAR(20),
    no_of_milestones_passed NVARCHAR(20),
    no_of_quizzes NVARCHAR(20),
    no_of_quizzes_submitted NVARCHAR(20),
    no_of_quizzes_passed NVARCHAR(20),
    no_of_tests NVARCHAR(20),
    no_of_tests_submissions NVARCHAR(20),
    no_of_tests_passed NVARCHAR(20),
    lms_overall_score NVARCHAR(20),
    is_lms_overall_score_higher_than_score_threshold NVARCHAR(20),
    lms_course_status NVARCHAR(50),
    ehub_course_status NVARCHAR(50),
    ehub_class_status NVARCHAR(50),
    is_course_paused NVARCHAR(20),
    course_leave_of_absence_reason NVARCHAR(MAX)
);

----------------------------------------------------------
-- Drop Table 'bronze.pw_learner_tracking_overview' if exists
----------------------------------------------------------
IF OBJECT_ID('bronze.pw_learner_tracking_overview', 'U') IS NOT NULL
    DROP TABLE bronze.pw_learner_tracking_overview;

CREATE TABLE bronze.pw_learner_tracking_overview(
    first_name NVARCHAR(50),
    last_name NVARCHAR(50),
    email NVARCHAR(100),
    course_name NVARCHAR(150),
    no_of_submissions NVARCHAR(50),
    no_assignments_passed NVARCHAR(50),
    lms_overall_score NVARCHAR(50),
    hubspot_id NVARCHAR(100),
    contact_phone NVARCHAR(50),
    age NVARCHAR(50),
    age_range NVARCHAR(50),
    country_of_residence NVARCHAR(50),
    category_country_of_residence NVARCHAR(50),
    city_of_residence NVARCHAR(50),
    cohort_name NVARCHAR(50),
    location_program_cohort NVARCHAR(100),
    learner_enrolled_into_lms NVARCHAR(50),
    has_logged_into_lms NVARCHAR(50),
    has_logged_into_ehub NVARCHAR(50),
    has_shown_up_in_circle NVARCHAR(50),
    has_shown_up_in_circle_during_the_last_two_days NVARCHAR(50),
    is_engaged_in_circle NVARCHAR(50),
    is_engaged_in_circle_during_the_last_two_days NVARCHAR(50),
    enrollment_program_activated NVARCHAR(50),
    class_name NVARCHAR(50),
    enrollment_course_activated NVARCHAR(50),
    is_program_withdrawn NVARCHAR(50),
    program_withdrawal_reason NVARCHAR(MAX),
    program_withdrawn_date NVARCHAR(50),
    is_course_deferred NVARCHAR(50),
    course_deferred_date NVARCHAR(50),
    on_track_for_on_time_graduation NVARCHAR(50),
    on_track_for_delayed_graduation NVARCHAR(50),
    no_of_assignments NVARCHAR(50),
    no_of_milestone NVARCHAR(50),
    no_of_milestone_submissions NVARCHAR(50),
    no_of_milestone_passed NVARCHAR(50),
    no_of_practice_quiz NVARCHAR(50),
    no_of_practice_quiz_submissions NVARCHAR(50),
    no_of_practice_quiz_passed NVARCHAR(50),
    no_of_test NVARCHAR(50),
    no_of_test_submissions NVARCHAR(50),
    no_of_test_passed NVARCHAR(50),
    assignments_accessed NVARCHAR(MAX),
    assignments_submitted NVARCHAR(MAX),
    assignments_missing NVARCHAR(MAX),
    assignments_passed NVARCHAR(MAX),
    assignments_submitted_and_failed NVARCHAR(MAX),
    learner_hub_city NVARCHAR(50),
    first_check_in NVARCHAR(50),
    most_recent_check_in NVARCHAR(50),
    total_number_of_check_in_any_hub NVARCHAR(50),
    no_of_check_in_most_visited_hub NVARCHAR(50),
    current_month_to_day_check_in NVARCHAR(50),
    check_ins_last_30_days NVARCHAR(50),
    meets_attendance_criteria NVARCHAR(50),
    most_visited_hub NVARCHAR(50),
    is_lms_overall_score_higher_than_threshold NVARCHAR(50),
    lms_course_status NVARCHAR(50),
    ehub_course_status NVARCHAR(50),
    ehub_class_status NVARCHAR(50),
    is_course_paused NVARCHAR(50),
    course_leave_of_absence_reason NVARCHAR(MAX),
    nile_cohort_name NVARCHAR(50)
);
