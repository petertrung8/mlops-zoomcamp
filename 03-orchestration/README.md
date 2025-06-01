# Airflow 3.0.1 Docker Compose Setup

This repository provides a Docker Compose setup for Apache Airflow 3.0.1, backed by PostgreSQL, with optional pgAdmin for database management for a zero-hassle local Airflow development environment.

### Features

* run airflow in docker
* manage all service in docker-compose 
* call docker images inside airflow DAG 

## 🚀 Step to run Airflow locally

Some of the steps are taken from the [Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) and [Michael Shoemaker's video](https://www.youtube.com/watch?v=PbSIVDou17Q).

### 1. Create your `.env` file

First,  we need to create the folders `dags`, `logs`, `config` and `plugins` in the target folder. Then we want to create an environment file `.env` in the same folder with `docker-compose.yaml` with the `AIRFLOW_UID`. In Linux, run the following commnands:

```
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

Afterwards you will need to edit the .env file to add Airflow, Postgres and pgadmin. Example how the file will look like is in `.env.example`, but you will add these following lines to the file, examples:

```
# Airflow configuration
AIRFLOW_WWW_USER_USERNAME=airflow
AIRFLOW_WWW_USER_PASSWORD=airflow

# PostgreSQL service setup
POSTGRES_PASSWORD=airflow
POSTGRES_USER=airflow
POSTGRES_DB=airflow

# pgAdmin service setup, it is used for visaulize the database
ENABLE_PGADMIN=true             # left empty to disable pgadmin view
COMPOSE_PROFILES=${ENABLE_PGADMIN:+pgadmin}
PGADMIN_DEFAULT_EMAIL=admin@admin.com
PGADMIN_DEFAULT_PASSWORD=admin
```

### 3.Launch services

To build the image of Airflow, run the following command

```
docker compose up -d
```

**Note**: The current `docker-compose.yaml` uses build option to also install custom dependencies in the `requirements.txt` file.

### 4. Access the UIs

* Airflow (API server & web UI): http://localhost:8080
  * Username: `airflow`
  * Password: `airflow`

* pgAdmin (if enabled): http://localhost:5050
  * Email: `admin@admin.com` 
  * Password: `admin`


## 📦 Project Structure

```
.
├── docker-compose.yaml     # Main Compose configuration
├── .env.example            # Template for environment variables
├── dags/                   # Your Airflow DAG definitions
├── logs/                   # Airflow scheduler & task logs
├── config/                 # airflow.cfg and custom configs
└── plugins/                # Custom Airflow plugins

```

## ⚙️ Configuration
### 1. Environment Variables

Use `.env.example` to suit your environment:

* **AIRFLOW_UID** — Host user ID for proper file ownership

* **AIRFLOW_WWW_USER_USERNAME** / **PASSWORD** — Web UI credentials

* **POSTGRES_USER** / **PASSWORD** / **DB** — Metadata database credentials

* **PGADMIN_DEFAULT_EMAIL** / **PASSWORD** — pgAdmin credentials


### 2. Compose Profiles

* Airflow
* PostgreSQL
* pgadmin

## 🛠️ Services
| Service                   | Description                                               |
| ------------------------- | --------------------------------------------------------- |
| **postgres**              | PostgreSQL metadata database                              |
| **pgadmin**               | Web UI for PostgreSQL                                     |
| **airflow-init**          | Initializes directories, permissions, and default configs |
| **airflow-scheduler**     | Schedules and triggers task execution                     |
| **airflow-apiserver**     | Exposes Airflow’s REST API and web UI                     |
| **airflow-dag-processor** | Parses and validates your DAG files                       |
| **airflow-triggerer**     | Handles deferrable and trigger-based operators            |
| **airflow-cli**           | CLI container for debugging (use `--profile debug`)       |

## 🔧 Customization
* Custom airflow.cfg — Drop overrides into config/ (mapped to /opt/airflow/config).

* Additional Python packages — Add them to `requirement.txt`.

* New DAGs — Simply place your .py files inside the dags/ folder; they’ll be auto-detected.