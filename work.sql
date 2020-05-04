-- Preferences Theme optimized (load themes with children)
EXPLAIN ANALYZE
WITH RECURSIVE preferences_theme_childs AS (
  SELECT theme_id,
         parent_id,
         name,
         on_moderation,
         CASE WHEN level = 1 THEN 'sub_theme'
              WHEN level = 2 THEN 'focus'
              ELSE 'skills_family'
         END AS theme_level
  FROM preferences_themes_parents
  INNER JOIN preferences_themes ON preferences_themes.id = theme_id
  WHERE parent_id = 1 AND preferences_themes.on_moderation = false

  UNION

  SELECT preferences_themes_parents.theme_id,
         preferences_themes_parents.parent_id,
         preferences_themes.name,
         preferences_themes.on_moderation,
         CASE WHEN level = 1 THEN 'sub_theme'
              WHEN level = 2 THEN 'focus'
              ELSE 'skills_family'
         END AS theme_level
  FROM preferences_themes_parents
  INNER JOIN preferences_themes ON preferences_themes.id = preferences_themes_parents.theme_id
  INNER JOIN preferences_theme_childs ON preferences_theme_childs.theme_id = preferences_themes_parents.parent_id
  WHERE preferences_themes.on_moderation = false
)
SELECT
  theme_level,
  json_agg(
    json_build_object(
      'id', theme_id,
      'name', name,
      'level', theme_level,
      'children', children,
      'on_moderation', on_moderation
    )::json
  ) AS themes
FROM preferences_theme_childs
LEFT OUTER JOIN (
  SELECT
    parent_id,
    json_agg(
      json_build_object(
        'id', theme_id,
        'level', theme_level
      )::json
    ) children
  FROM preferences_theme_childs
  GROUP BY parent_id
  ) preferences_theme_children ON preferences_theme_children.parent_id = preferences_theme_childs.theme_id
GROUP BY theme_level;

-- Admin dashboard suggestions by weeks without N+1
SELECT date_trunc('week', created_at::date) AS week,
       json_agg(
          json_build_object(
            'id', suggested_candidates_logs.user_id,
            'count', suggested_candidates_logs.logs_count
          )
        ) AS all_suggested_candidates_by_user,
       json_agg(
         json_build_object(
           'id', visible_suggested_candidates_logs.user_id,
           'count', visible_suggested_candidates_logs.logs_count
         )
       ) AS manually_suggested_visible_candidates_by_user
FROM job_application_logs
LEFT OUTER JOIN (
  SELECT user_id, date_trunc('week', created_at::date) AS week, COUNT(id) AS logs_count
  FROM job_application_logs
  WHERE job_application_logs.action = 'suggested' AND user_id IS NOT NULL
  GROUP BY week, user_id
) suggested_candidates_logs ON suggested_candidates_logs.week = date_trunc('week', job_application_logs.created_at::date)
LEFT OUTER JOIN (
  SELECT job_application_logs.user_id, date_trunc('week', job_application_logs.created_at::date) AS week, COUNT(job_application_logs.id) AS logs_count
  FROM job_application_logs
  INNER JOIN job_applications ja on job_application_logs.job_application_id = ja.id
  WHERE ja.state <> 'deleted' AND job_application_logs.action = 'suggested' AND job_application_logs.user_id IS NOT NULL
  GROUP BY week, job_application_logs.user_id
) visible_suggested_candidates_logs ON visible_suggested_candidates_logs.week = date_trunc('week', job_application_logs.created_at::date)
WHERE created_at > '2020-03-02' AND action = 'suggested'
GROUP BY date_trunc('week', job_application_logs.created_at::date);

-- Freelancer jobs without N+1 - solution 1
EXPLAIN ANALYZE
WITH latest_job_applications AS (
  SELECT job_applications.id AS id, job_applications.job_id AS job_id, job_applications_freelancers.freelancer_id AS freelancer_id
  FROM job_applications
  INNER JOIN job_applications_freelancers ON job_applications_freelancers.application_id = job_applications.id
  WHERE job_applications.created_at > '2019-11-27 17:22:39.550782'
), freelancer_shortlisted_jobs AS (
  SELECT ja.job_id AS id, ja.created_at AS created_at
  FROM jobs
  INNER JOIN job_steps ON job_steps.job_id = jobs.id AND job_steps.name <> 'Suggested' AND job_steps.name <> 'Applied'
  INNER JOIN job_applications AS ja ON jobs.id = ja.job_id AND ja.step_id = job_steps.id
  INNER JOIN job_applications_freelancers AS jaf ON jaf.application_id = ja.id
)
SELECT jobs.*,
       count(DISTINCT latest_job_applications.freelancer_id) AS active_freelancers_ids,
       max(DISTINCT freelancer_shortlisted_jobs.created_at) AS freelancer_apply_shortlisted_at
FROM jobs
LEFT OUTER JOIN freelancer_shortlisted_jobs ON freelancer_shortlisted_jobs.id = jobs.id
LEFT OUTER JOIN latest_job_applications ON latest_job_applications.job_id = jobs.id
WHERE jobs.id = 1
GROUP BY jobs.id;

-- Simple count query for large data set
EXPLAIN ANALYZE
SELECT COUNT(*) AS count
FROM job_application_logs;

-- Faster count for large data sets (Job::Application::Log)
EXPLAIN ANALYZE
SELECT COUNT(*) * 10 AS count
FROM job_application_logs TABLESAMPLE SYSTEM(10);