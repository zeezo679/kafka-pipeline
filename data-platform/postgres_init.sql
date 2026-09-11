-- Runs automatically on first container init only (docker-entrypoint-initdb.d).
-- POSTGRES_DB/POSTGRES_USER/POSTGRES_PASSWORD env vars already create the
-- `metastore` database and `hiveuser` role — this script only adds Hue's DB.
 
CREATE DATABASE huedb;
CREATE USER huedbuser WITH PASSWORD 'huedbpassword';
GRANT ALL PRIVILEGES ON DATABASE huedb TO huedbuser;
 
-- Postgres 15+ revokes CREATE on public schema from non-owners by default.
-- Harmless no-op on 13, but keeps this script future-proof if you bump the image.
\c huedb
GRANT ALL ON SCHEMA public TO huedbuser;