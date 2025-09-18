/*
================================================================================
Load and Clean Learner Activity Table (Silver Layer)
================================================================================
Script Purpose:
    - Clean up string fields by removing unwanted spaces (using TRIM).
    - Standardize phone numbers into a consistent format with +251 prefix.
    - Convert text-based numeric fields (age, scores, counts, dates) into
      the correct data types (INT, FLOAT, DATETIME).
    - Prepare the cleaned dataset from 'bronze.pw_learner_activity' to be
      stored in the 'Silver' layer for downstream analytics.

    Run this script to re-define and clean the DDL structure of the Silver
    learner activity and learner tracking overview tables.
*/


USE AlxDataWareHouse;
GO

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @batch_start_time DATETIME;
    DECLARE @batch_end_time DATETIME;
    DECLARE @start_time DATETIME;
    DECLARE @end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '============================================================';
        PRINT 'Loading Silver Layer';
        PRINT '============================================================';

        -- Load pw_learner_activity
        PRINT '-------------------------------------------------------------';
        PRINT 'Loading Silver Learner Activity';
        PRINT '-------------------------------------------------------------';

        SET @start_time = GETDATE();
        TRUNCATE TABLE silver.pw_learner_activity;

        INSERT INTO silver.pw_learner_activity(
            first_name,
            last_name,
            email,
            course_name,
            assignment_name,
            is_assignment_accessed,
            is_assignment_submitted,
            is_assignment_passed,
            contact_phone,
            age,
            age_range,
            country_of_residence,
            category_country_of_residences,
            city_of_residence,
            day_of_activation,
            cohort_name,
            class_name,
            location_program_cohort,
            assignment_type,
            assignment_score,
            learner_enrolled_into_lms,
            has_logged_into_lms,
            has_logged_into_eHub,
            has_shown_up_in_circle,
            has_shown_up_in_circle_during_the_last_two_days,
            is_engaged_in_circle,
            is_engaged_in_circle_during_the_last_two_days,
            enrollment_program_activated,
            enrollment_in_previous_withdrawn_class,
            enrollment_course_activated,
            is_program_withdrawn,
            program_withdrawal_reason,
            program_withdrawn_date,
            is_course_deferred,
            course_deferred_date,
            on_track_for_on_time_graduation,
            on_track_for_delayed_graduation,
            is_assignment_resubmitted,
            is_assignment_resubmitted_and_not_corrected_yet,
            is_assignment_graded,
            is_assignment_corrected_by_an_external_grader,
            corrected_external_grader_name,
            corrected_external_grader_email,
            is_on_track,
            no_of_assignment_submissions,
            submission_link,
            no_of_assignmnets,
            no_of_submissions,
            no_of_assignments_passed,
            no_of_milestone,
            no_of_milestone_submissions,
            no_of_milestones_passed,
            no_of_quizzes,
            no_of_quizzes_submitted,
            no_of_quizzes_passed,
            no_of_tests,
            no_of_tests_submissions,
            no_of_tests_passed,
            lms_overall_score,
            is_lms_overall_score_higher_than_score_threshold,
            lms_course_status,
            ehub_course_status,
            ehub_class_status,
            is_course_paused,
            course_leave_of_absence_reason
        )
        SELECT
            TRIM(first_name),
            TRIM(last_name),
            TRIM(email),
            TRIM(course_name),
            TRIM(assignment_name),
            TRIM(is_assignment_accessed),
            TRIM(is_assignment_submitted),
            TRIM(is_assignment_passed),
            TRIM(
                CASE 
                    WHEN contact_phone LIKE '+251%' THEN contact_phone
                    WHEN contact_phone LIKE '251%'  THEN CONCAT('+', contact_phone)
                    WHEN contact_phone LIKE '0%'    THEN CONCAT('+251', SUBSTRING(contact_phone, 2, LEN(contact_phone)))
                    ELSE CONCAT('+251', contact_phone)
                END
            ),
            TRY_CAST(TRIM(age) AS INT),
            TRIM(age_range),
            TRIM(country_of_residence),
            TRIM(category_country_of_residences),
            TRIM(city_of_residence),
            TRY_CAST(TRIM(day_of_activation) AS INT),
            TRIM(cohort_name),
            TRIM(class_name),
            TRIM(location_program_cohort),
            TRIM(assignment_type),
            TRY_CAST(TRIM(assignment_score) AS FLOAT),
            TRIM(learner_enrolled_into_lms),
            TRIM(has_logged_into_lms),
            TRIM(has_logged_into_eHub),
            TRIM(has_shown_up_in_circle),
            TRIM(has_shown_up_in_circle_during_the_last_two_days),
            TRIM(is_engaged_in_circle),
            TRIM(is_engaged_in_circle_during_the_last_two_days),
            TRIM(enrollment_program_activated),
            TRIM(enrollment_in_previous_withdrawn_class),
            TRIM(enrollment_course_activated),
            TRIM(is_program_withdrawn),
            TRIM(program_withdrawal_reason),
            TRY_CAST(TRIM(program_withdrawn_date) AS DATETIME),
            TRIM(is_course_deferred),
            TRY_CAST(TRIM(course_deferred_date) AS DATETIME),
            TRIM(on_track_for_on_time_graduation),
            TRIM(on_track_for_delayed_graduation),
            TRIM(is_assignment_resubmitted),
            TRIM(is_assignment_resubmitted_and_not_corrected_yet),
            TRIM(is_assignment_graded),
            TRIM(is_assignment_corrected_by_an_external_grader),
            TRIM(corrected_external_grader_name),
            TRIM(corrected_external_grader_email),
            TRIM(is_on_track),
            TRY_CAST(TRIM(no_of_assignment_submissions) AS INT),
            TRIM(submission_link),
            TRY_CAST(TRIM(no_of_assignmnets) AS INT),
            TRY_CAST(TRIM(no_of_submissions) AS INT),
            TRY_CAST(TRIM(no_of_assignments_passed) AS INT),
            TRY_CAST(TRIM(no_of_milestone) AS INT),
            TRY_CAST(TRIM(no_of_milestone_submissions) AS INT),
            TRY_CAST(TRIM(no_of_milestones_passed) AS INT),
            TRY_CAST(TRIM(no_of_quizzes) AS INT),
            TRY_CAST(TRIM(no_of_quizzes_submitted) AS INT),
            TRY_CAST(TRIM(no_of_quizzes_passed) AS INT),
            TRY_CAST(TRIM(no_of_tests) AS INT),
            TRY_CAST(TRIM(no_of_tests_submissions) AS INT),
            TRY_CAST(TRIM(no_of_tests_passed) AS INT),
            TRY_CAST(TRIM(lms_overall_score) AS FLOAT),
            TRIM(is_lms_overall_score_higher_than_score_threshold),
            TRIM(lms_course_status),
            TRIM(ehub_course_status),
            TRIM(ehub_class_status),
            TRIM(is_course_paused),
            TRIM(course_leave_of_absence_reason)
        FROM bronze.pw_learner_activity;

        -- Load pw_learner_tracking_overview
        PRINT '-------------------------------------------------------------';
        PRINT 'Loading Silver Learner Tracking Overview';
        PRINT '-------------------------------------------------------------';

        SET @start_time = GETDATE();
        TRUNCATE TABLE silver.pw_learner_tracking_overview;

        INSERT INTO silver.pw_learner_tracking_overview(
            first_name,
            last_name,
            email,
            course_name,
            no_of_submissions,
            no_assignments_passed,
            lms_overall_score,
            hubspot_id,
            contact_phone,
            age,
            age_range,
            country_of_residence,
            category_country_of_residence,
            city_of_residence,
            cohort_name,
            location_program_cohort,
            learner_enrolled_into_lms,
            has_logged_into_lms,
            has_logged_into_eHub,
            has_shown_up_in_circle,
            has_shown_up_in_circle_during_the_last_two_days,
            is_engaged_in_circle,
            is_engaged_in_circle_during_the_last_two_days,
            enrollment_program_activated,
            class_name,
            enrollment_course_activated,
            is_program_withdrawn,
            program_withdrawal_reason,
            program_withdrawn_date,
            is_course_deferred,
            course_deferred_date,
            on_track_for_on_time_graduation,
            on_track_for_delayed_graduation,
            no_of_assignments,
            no_of_milestone,
            no_of_milestone_submissions,
            no_of_milestone_passed,
            no_of_practice_quiz,
            no_of_practice_quiz_submissions,
            no_of_practice_quiz_passed,
            no_of_test,
            no_of_test_submissions,
            no_of_test_passed,
            assignments_accessed,
            assignments_submitted,
            assignments_missing,
            assignments_passed,
            assignments_submitted_and_failed,
            learner_hub_city,
            first_check_in,
            most_recent_check_in,
            total_number_of_check_in_any_hub,
            no_of_check_in_most_visited_hub,
            current_month_to_day_check_in,
            check_ins_last_30_days,
            meets_attendance_criteria,
            most_visited_hub,
            is_lms_overall_score_higher_than_threshold,
            lms_course_status,
            ehub_course_status,
            ehub_class_status,
            is_course_paused,
            course_leave_of_absence_reason,
            nile_cohort_name
        )
        SELECT
            TRIM(first_name),
            TRIM(last_name),
            TRIM(email),
            TRIM(course_name),
            TRY_CAST(TRIM(no_of_submissions) AS INT),
            TRY_CAST(TRIM(no_assignments_passed) AS INT),
            TRY_CAST(TRIM(lms_overall_score) AS FLOAT),
            TRIM(hubspot_id),
            TRIM(
                CASE 
                    WHEN contact_phone LIKE '+251%' THEN contact_phone
                    WHEN contact_phone LIKE '251%'  THEN CONCAT('+', contact_phone)
                    WHEN contact_phone LIKE '0%'    THEN CONCAT('+251', SUBSTRING(contact_phone, 2, LEN(contact_phone)))
                    ELSE CONCAT('+251', contact_phone)
                END
            ),
            TRY_CAST(TRIM(age) AS INT),
            TRIM(age_range),
            TRIM(country_of_residence),
            TRIM(category_country_of_residence),
            TRIM(city_of_residence),
            TRIM(cohort_name),
            TRIM(location_program_cohort),
            TRIM(learner_enrolled_into_lms),
            TRIM(has_logged_into_lms),
            TRIM(has_logged_into_eHub),
            TRIM(has_shown_up_in_circle),
            TRIM(has_shown_up_in_circle_during_the_last_two_days),
            TRIM(is_engaged_in_circle),
            TRIM(is_engaged_in_circle_during_the_last_two_days),
            TRIM(enrollment_program_activated),
            TRIM(class_name),
            TRIM(enrollment_course_activated),
            TRIM(is_program_withdrawn),
            TRIM(program_withdrawal_reason),
            TRY_CAST(TRIM(program_withdrawn_date) AS DATETIME),
            TRIM(is_course_deferred),
            TRY_CAST(TRIM(course_deferred_date) AS DATETIME),
            TRIM(on_track_for_on_time_graduation),
            TRIM(on_track_for_delayed_graduation),
            TRY_CAST(TRIM(no_of_assignments) AS INT),
            TRY_CAST(TRIM(no_of_milestone) AS INT),
            TRY_CAST(TRIM(no_of_milestone_submissions) AS INT),
            TRY_CAST(TRIM(no_of_milestone_passed) AS INT),
            TRY_CAST(TRIM(no_of_practice_quiz) AS INT),
            TRY_CAST(TRIM(no_of_practice_quiz_submissions) AS INT),
            TRY_CAST(TRIM(no_of_practice_quiz_passed) AS INT),
            TRY_CAST(TRIM(no_of_test) AS INT),
            TRY_CAST(TRIM(no_of_test_submissions) AS INT),
            TRY_CAST(TRIM(no_of_test_passed) AS INT),
            TRIM(assignments_accessed),
            TRIM(assignments_submitted),
            TRIM(assignments_missing),
            TRIM(assignments_passed),
            TRIM(assignments_submitted_and_failed),
            TRIM(learner_hub_city),
            TRY_CAST(TRIM(first_check_in) AS DATETIME),
            TRY_CAST(TRIM(most_recent_check_in) AS DATETIME),
            TRY_CAST(TRIM(total_number_of_check_in_any_hub) AS INT),
            TRY_CAST(TRIM(no_of_check_in_most_visited_hub) AS INT),
            TRY_CAST(TRIM(current_month_to_day_check_in) AS INT),
            TRY_CAST(TRIM(check_ins_last_30_days) AS INT),
            TRIM(meets_attendance_criteria),
            TRIM(most_visited_hub),
            TRIM(is_lms_overall_score_higher_than_threshold),
            TRIM(lms_course_status),
            TRIM(ehub_course_status),
            TRIM(ehub_class_status),
            TRIM(is_course_paused),
            TRIM(course_leave_of_absence_reason),
            TRIM(nile_cohort_name)
        FROM bronze.pw_learner_tracking_overview;

        SET @batch_end_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer is Completed.';
        PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        PRINT '============================================================';
        PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '============================================================';
    END CATCH
END;
