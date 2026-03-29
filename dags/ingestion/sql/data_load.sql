-- 1. Load products table
COPY raw.products
FROM 's3://dec-capstone-joshua-raw-data/raw/products/'
IAM_ROLE 'arn:aws:iam::711266489700:role/dec-capstone-joshua-redshift-role'
FORMAT AS PARQUET
COMPUPDATE OFF
STATUPDATE OFF;

-- 2. Load suppliers table
COPY raw.suppliers
FROM 's3://dec-capstone-joshua-raw-data/raw/suppliers/'
IAM_ROLE 'arn:aws:iam::711266489700:role/dec-capstone-joshua-redshift-role'
FORMAT AS PARQUET
COMPUPDATE OFF
STATUPDATE OFF;

-- 3. Load warehouses table
COPY raw.warehouses
FROM 's3://dec-capstone-joshua-raw-data/raw/warehouses/'
IAM_ROLE 'arn:aws:iam::711266489700:role/dec-capstone-joshua-redshift-role'
FORMAT AS PARQUET
COMPUPDATE OFF
STATUPDATE OFF;

-- 4. Load stores table
COPY raw.stores
FROM 's3://dec-capstone-joshua-raw-data/raw/stores/'
IAM_ROLE 'arn:aws:iam::711266489700:role/dec-capstone-joshua-redshift-role'
FORMAT AS PARQUET
COMPUPDATE OFF
STATUPDATE OFF;

-- 5. Load inventory table
COPY raw.inventory
FROM 's3://dec-capstone-joshua-raw-data/raw/inventory/'
IAM_ROLE 'arn:aws:iam::711266489700:role/dec-capstone-joshua-redshift-role'
FORMAT AS PARQUET
COMPUPDATE OFF
STATUPDATE OFF;

-- 6. Load shipments table
COPY raw.shipments
FROM 's3://dec-capstone-joshua-raw-data/raw/shipments/'
IAM_ROLE 'arn:aws:iam::711266489700:role/dec-capstone-joshua-redshift-role'
FORMAT AS PARQUET
COMPUPDATE OFF
STATUPDATE OFF;

-- 7. Load sales table
COPY raw.sales
FROM 's3://dec-capstone-joshua-raw-data/raw/sales/'
IAM_ROLE 'arn:aws:iam::711266489700:role/dec-capstone-joshua-redshift-role'
FORMAT AS PARQUET
COMPUPDATE OFF
STATUPDATE OFF;