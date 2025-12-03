#!/bin/bash
set -e

# Create additional databases and users for Airflow and Superset

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Create Airflow database and user
    CREATE USER airflow WITH PASSWORD 'airflow';
    CREATE DATABASE airflow;
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
    ALTER DATABASE airflow OWNER TO airflow;

    -- Create Superset database and user
    CREATE USER superset WITH PASSWORD 'superset';
    CREATE DATABASE superset;
    GRANT ALL PRIVILEGES ON DATABASE superset TO superset;
    ALTER DATABASE superset OWNER TO superset;

    -- Grant schema permissions
    \c airflow
    GRANT ALL ON SCHEMA public TO airflow;
    
    \c superset
    GRANT ALL ON SCHEMA public TO superset;
EOSQL

echo "Additional databases created successfully!"

