// Creation de la base de données linkedin
create or replace database linkedin;

// Creation du schema public
create or replace schema public;

// Creation du stage vers le S3 public
CREATE OR REPLACE STAGE linkedin_stage
URL='s3://snowflake-lab-bucket/';


// Ceation des file format en CSV et JSON
CREATE OR REPLACE FILE FORMAT csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1;

CREATE OR REPLACE FILE FORMAT json_format
TYPE = 'JSON'
STRIP_OUTER_ARRAY = TRUE;

// Creation des tables
-- JOB_POSTINGS
CREATE OR REPLACE TABLE job_postings (
    job_id STRING,
    company_name STRING,
    title STRING,
    description STRING,
    max_salary FLOAT,
    med_salary FLOAT,
    min_salary FLOAT,
    pay_period STRING,
    formatted_work_type STRING,
    location STRING,
    applies INT,
    original_listed_time STRING,
    remote_allowed INT,
    views INT,
    job_posting_url STRING,
    application_url STRING,
    application_type STRING,
    expiry STRING,
    closed_time STRING,
    formatted_experience_level STRING,
    skills_desc STRING,
    listed_time STRING,
    posting_domain STRING,
    sponsored BOOLEAN,
    work_type STRING,
    currency STRING,
    compensation_type STRING
);

-- BENEFITS
CREATE OR REPLACE TABLE benefits (
    job_id STRING,
    inferred BOOLEAN,
    type STRING
);

-- COMPANIES
CREATE OR REPLACE TABLE companies (
    company_id STRING,
    name STRING,
    description STRING,
    company_size INT,
    state STRING,
    country STRING,
    city STRING,
    zip_code STRING,
    address STRING,
    url STRING
);

-- EMPLOYEE_COUNTS
CREATE OR REPLACE TABLE employee_counts (
    company_id STRING,
    employee_count INT,
    follower_count INT,
    time_recorded STRING
);

-- JOB_SKILLS
CREATE OR REPLACE TABLE job_skills (
    job_id STRING,
    skill_abr STRING
);

-- JOB_INDUSTRIES
CREATE OR REPLACE TABLE job_industries (
    job_id STRING,
    industry_id STRING
);

-- COMPANY_SPECIALITIES
CREATE OR REPLACE TABLE company_specialities (
    company_id STRING,
    speciality STRING
);

-- COMPANY_INDUSTRIES
CREATE OR REPLACE TABLE company_industries (
    company_id STRING,
    industry STRING
);

// Copier les données du S3 public vers les tables pour les fichiers CSV
COPY INTO linkedin.public.job_postings
FROM 's3://snowflake-lab-bucket/job_postings.csv'
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
ON_ERROR = 'SKIP_FILE';

COPY INTO linkedin.public.benefits
FROM 's3://snowflake-lab-bucket/benefits.csv'
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
ON_ERROR = 'SKIP_FILE';

COPY INTO linkedin.public.employee_counts 
FROM 's3://snowflake-lab-bucket/employee_counts.csv'
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
ON_ERROR = 'SKIP_FILE';

COPY INTO linkedin.public.job_skills 
FROM 's3://snowflake-lab-bucket/job_skills.csv'
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
ON_ERROR = 'SKIP_FILE';


// Copier les données du S3 public vers les tables pour les fichers JSON
// Creation d'une table temporaire pour copier les données du JSON
CREATE OR REPLACE TABLE raw_companies (
    json_data VARIANT
);

// Copier les données du S3 JSON vers la table temporaire
COPY INTO raw_companies(json_data)
FROM 's3://snowflake-lab-bucket/companies.json'
FILE_FORMAT = (
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
)
ON_ERROR = 'SKIP_FILE';

// SELECT pour vérifier que les données on bien était copier
SELECT * FROM raw_companies;

// Copie des données de la table temporaire vers la table définitive
CREATE OR REPLACE TABLE linkedin.public.companies AS
SELECT
    json_data:company_id::STRING AS company_id,
    json_data:name::STRING AS name,
    json_data:description::STRING AS description,
    json_data:company_size::INT AS company_size,
    json_data:state::STRING AS state,
    json_data:country::STRING AS country,
    json_data:city::STRING AS city,
    json_data:zip_code::STRING AS zip_code,
    json_data:address::STRING AS address,
    json_data:url::STRING AS url
FROM raw_companies;

// Supprimer la table temporaire
drop table raw_companies;

CREATE OR REPLACE TABLE raw_job_industries (
    json_data VARIANT
);

COPY INTO raw_job_industries(json_data)
FROM 's3://snowflake-lab-bucket/job_industries.json'
FILE_FORMAT = (
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
)
ON_ERROR = 'SKIP_FILE';

CREATE OR REPLACE TABLE linkedin.public.job_industries AS
SELECT
    json_data:job_id::STRING AS job_id,
    json_data:industry_id::STRING AS industry_id
FROM raw_job_industries;

drop table raw_job_industries;

CREATE OR REPLACE TABLE raw_company_specialities (
    json_data VARIANT
);

COPY INTO raw_company_specialities(json_data)
FROM 's3://snowflake-lab-bucket/company_specialities.json'
FILE_FORMAT = (
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
)
ON_ERROR = 'SKIP_FILE';

CREATE OR REPLACE TABLE linkedin.public.company_specialities AS
SELECT
    json_data:company_id::STRING AS company_id,
    json_data:speciality::STRING AS speciality
FROM raw_company_specialities;

drop table raw_company_specialities;

CREATE OR REPLACE TABLE raw_company_industries (
    json_data VARIANT
);

COPY INTO raw_company_industries(json_data)
FROM 's3://snowflake-lab-bucket/company_industries.json'
FILE_FORMAT = (
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
)
ON_ERROR = 'SKIP_FILE';

CREATE OR REPLACE TABLE linkedin.public.company_industries AS
SELECT
    json_data:company_id::STRING AS company_id,
    json_data:industry::STRING AS industry
FROM raw_company_industries;

drop table raw_company_industries;


// Tous les Select pour verifier que les tables sont bien remplit
SELECT * from job_postings;
SELECT * from benefits;
SELECT * from companies;
SELECT * from employee_counts;
SELECT * from job_skills;
SELECT * from job_industries;
SELECT * from company_specialities;
SELECT * from company_industries;
LIST @linkedin_stage;


// Analyses SQL

// 1) Top 10 des titres de postes les plus publiés par industrie.

SELECT
  ji.industry_id,
  jp.title,
  COUNT(*) AS nb_postes
FROM job_postings jp
JOIN job_industries ji ON jp.job_id = ji.job_id
GROUP BY ji.industry_id, jp.title
ORDER BY ji.industry_id, nb_postes DESC
LIMIT 10;


// 2) Top 10 des postes les mieux rémunérés par industrie

SELECT
  ji.industry_id,
  jp.title,
  MAX(TRY_CAST(jp.max_salary AS FLOAT)) AS max_salary
FROM job_postings jp
JOIN job_industries ji ON jp.job_id = ji.job_id
WHERE jp.max_salary IS NOT NULL
GROUP BY ji.industry_id, jp.title
ORDER BY ji.industry_id, max_salary DESC
LIMIT 10;


// 3) Répartition des offres d’emploi par taille d’entreprise

SELECT
  c.company_size,
  COUNT(*) AS nb_offres
FROM job_postings jp
JOIN companies c
  ON TRY_CAST(jp.company_name AS INT) = TRY_CAST(c.company_id AS INT)
GROUP BY c.company_size
ORDER BY c.company_size;


// 4) Répartition des offres par secteur d’activité

SELECT
  ji.industry_id,
  COUNT(*) AS nb_offres
FROM job_postings jp
JOIN job_industries ji ON jp.job_id = ji.job_id
GROUP BY ji.industry_id
ORDER BY nb_offres DESC;


// 5) Répartition des offres par type d’emploi (temps plein, partiel, etc.)

SELECT
  jp.formatted_work_type,
  COUNT(*) AS nb_offres
FROM job_postings jp
GROUP BY jp.formatted_work_type
ORDER BY nb_offres DESC;
