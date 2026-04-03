-- create raw schema to hold ingested data
CREATE SCHEMA IF NOT EXISTS "raw";

-- create staging schema for cleaned data
CREATE SCHEMA IF NOT EXISTS staging;

-- create marts schema for final analytical tables
CREATE SCHEMA IF NOT EXISTS marts;