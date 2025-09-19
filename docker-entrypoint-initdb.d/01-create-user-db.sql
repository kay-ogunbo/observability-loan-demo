-- Create the application role and database only if they don't already exist

DO
$$
BEGIN
   -- Create role loan_admin if missing
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles WHERE rolname = 'loan_admin'
   ) THEN
      CREATE ROLE loan_admin WITH LOGIN PASSWORD 'loan_pass';
   END IF;
END
$$;

DO
$$
BEGIN
   -- Create database loanfintech if missing and set owner
   IF NOT EXISTS (
      SELECT FROM pg_database WHERE datname = 'loanfintech'
   ) THEN
      CREATE DATABASE loanfintech OWNER loan_admin;
   END IF;
END
$$;

-- Grant privileges even if role/db already existed
GRANT ALL PRIVILEGES ON DATABASE loanfintech TO loan_admin;
