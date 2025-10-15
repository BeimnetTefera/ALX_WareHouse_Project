USE AlxDataWareHouse;
GO

-- Clear out existing data
TRUNCATE TABLE silver.pw_learner_tracking_overview;
GO

-- Insert the pivoted data
INSERT INTO silver.pw_learner_tracking_overview (
    first_name,
    lms_overall_score,
    enrollment_course_activated,
    is_program_withdrawn,
    program_withdrawal_reason,
    program_withdrawn_date,
    is_course_deferred,
    course_deferred_date,
    is_lms_overall_score_higher_than_threshold,
    is_course_paused,
    course_leave_of_absence_reason,

    -- Pivoted course columns
    BUS200_no_of_submissions,
    BUS200_no_assignments_passed,
    BUS200_lms_course_status,

    ENT110_no_of_submissions,
    ENT110_no_assignments_passed,
    ENT110_lms_course_status,

    QNT102_no_of_submissions,
    QNT102_no_assignments_passed,
    QNT102_lms_course_status,

    SE101_no_of_submissions,
    SE101_no_assignments_passed,
    SE101_lms_course_status,

    SE102_no_of_submissions,
    SE102_no_assignments_passed,
    SE102_lms_course_status
)
SELECT
    base.first_name,
    MAX(base.lms_overall_score) AS lms_overall_score,
    MAX(base.enrollment_course_activated) AS enrollment_course_activated,
    MAX(base.is_program_withdrawn) AS is_program_withdrawn,
    MAX(base.program_withdrawal_reason) AS program_withdrawal_reason,
    MAX(base.program_withdrawn_date) AS program_withdrawn_date,
    MAX(base.is_course_deferred) AS is_course_deferred,
    MAX(base.course_deferred_date) AS course_deferred_date,
    MAX(base.is_lms_overall_score_higher_than_threshold) AS is_lms_overall_score_higher_than_threshold,
    MAX(base.is_course_paused) AS is_course_paused,
    MAX(base.course_leave_of_absence_reason) AS course_leave_of_absence_reason,

    -- === Pivot each course ===
    MAX(CASE WHEN base.course_name = 'BUS 200: Business Finance' THEN base.no_of_submissions END) AS BUS200_no_of_submissions,
    MAX(CASE WHEN base.course_name = 'BUS 200: Business Finance' THEN base.no_assignments_passed END) AS BUS200_no_assignments_passed,
    MAX(CASE WHEN base.course_name = 'BUS 200: Business Finance' THEN base.lms_course_status END) AS BUS200_lms_course_status,

    MAX(CASE WHEN base.course_name = 'ENT 110: Introduction to Venture Creation' THEN base.no_of_submissions END) AS ENT110_no_of_submissions,
    MAX(CASE WHEN base.course_name = 'ENT 110: Introduction to Venture Creation' THEN base.no_assignments_passed END) AS ENT110_no_assignments_passed,
    MAX(CASE WHEN base.course_name = 'ENT 110: Introduction to Venture Creation' THEN base.lms_course_status END) AS ENT110_lms_course_status,

    MAX(CASE WHEN base.course_name = 'QNT 102: Introduction to Statistics' THEN base.no_of_submissions END) AS QNT102_no_of_submissions,
    MAX(CASE WHEN base.course_name = 'QNT 102: Introduction to Statistics' THEN base.no_assignments_passed END) AS QNT102_no_assignments_passed,
    MAX(CASE WHEN base.course_name = 'QNT 102: Introduction to Statistics' THEN base.lms_course_status END) AS QNT102_lms_course_status,

    MAX(CASE WHEN base.course_name = 'SE 101: Introduction to Computing' THEN base.no_of_submissions END) AS SE101_no_of_submissions,
    MAX(CASE WHEN base.course_name = 'SE 101: Introduction to Computing' THEN base.no_assignments_passed END) AS SE101_no_assignments_passed,
    MAX(CASE WHEN base.course_name = 'SE 101: Introduction to Computing' THEN base.lms_course_status END) AS SE101_lms_course_status,

    MAX(CASE WHEN base.course_name = 'SE 102: Foundations of Linux and Version Control' THEN base.no_of_submissions END) AS SE102_no_of_submissions,
    MAX(CASE WHEN base.course_name = 'SE 102: Foundations of Linux and Version Control' THEN base.no_assignments_passed END) AS SE102_no_assignments_passed,
    MAX(CASE WHEN base.course_name = 'SE 102: Foundations of Linux and Version Control' THEN base.lms_course_status END) AS SE102_lms_course_status

FROM bronze.pw_learner_tracking_overview AS base
GROUP BY base.first_name,base.email;
GO
