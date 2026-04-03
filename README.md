# Design and Implementation of SupplyChain360 Unified Data Platform

## Project Overview

This project involves the design and implementation of a unified data platform for SupplyChain360, a fast-growing retail distribution company in the United States. The platform integrates operational data from multiple fragmented systems including warehouse inventory, logistics shipment records, supplier delivery logs, and store sales data into a centralized, analytics-ready data warehouse.
The platform enables the company to answer critical business questions such as:

1. Which products are causing the most stockouts?
2. Which warehouses are inefficient?
3. Which suppliers consistently deliver late?
4. What are the regional sales demand patterns?

By centralizing this data, the platform supports better inventory planning, supplier performance monitoring, shipment tracking and demand forecasting which can potentially saving millions of dollars annually.

## Project Structure
- `dags/`: Contains Airflow DAGs and SQL scripts for data ingestion and transformation.
  - `dags/supplychain360_dag.py`: Airflow DAG for orchestrating the data pipeline.
  - `dags/initialize_redshift.py`: Airflow DAG to initialize Redshift for efficient data loading and trigger supplychain360 DAG.
  - `dags/ingestion/`: Python scripts for ingesting data from various sources.
  - `include/sql/`: SQL scripts for creating Redshift schemas.
  - `dbt_project/models/`: DBT models for transforming and aggregating data.
- `infrastructure/`: Infrastructure as Code (IaC) scripts for setting up AWS resources using Terraform.
- `Dockerfile`: Docker configuration for the custom image.
- `requirements.txt`: Python dependencies for the project.
- `Docker-compose.yml`: Docker Compose configuration for local development and testing.
- `.github/workflows/`: GitHub Actions workflows for CI/CD.
   - `.github/workflows/ci.yml`: CI workflow that carries out checks on code linting to ensure code written follows best practices.
   - `.github/workflows/cd.yml`: CD workflow for deploying the project to Docker Hub.

## Architecture Diagram

![Architecture Diagram](./architecture_diagram.png)

## Getting Started

To set up and run the Supplychain360 Data Platform, follow these steps:

1. **Clone the Repository**: Clone this repository to your local machine.
2. **Set Up AWS Resources**: Use the Terraform scripts in the `infrastructure/` directory to provision the necessary AWS resources. The AWS credentials should be configured in your environment. You can leverage `aws configure` to set up your credentials.
3. **Pull the project image from Docker Hub**: Pull the project image from Docker Hub using the following command:
   ```
   docker pull ffemiml/joshua-dec-capstone-pipeline:latest
   ```
   Then run the docker-compose to start the services:
   ```
   docker-compose up -d
   ```
4. **Configure Airflow Connections**: Set up Airflow and configure the connections to AWS services. The required connections include:
   - `aws_source`: AWS credentials for accessing source S3 bucket and parameter store.
   - `aws_dest`: AWS credentials for accessing destination S3 bucket.
   - `redshift_conn`: Connection details for the Redshift cluster.
   - `google_cloud_conn`: Connection details for Google Cloud services (google spreadsheets).
5. **Run Airflow DAGs**: There are two main DAGs to run:
   - `initialize_redshift`: This DAG initializes Redshift AutoCopy for efficient data loading and triggers the `supplychain360_dag` DAG.
   - `supplychain360_dag`: This DAG orchestrates the data ingestion, transformation, and loading processes.
6. **Monitor and Maintain**: Use Airflow's UI to monitor the DAG runs and ensure that the data pipeline is functioning correctly.
7. **Create PowerBI dashboard**: Connect redshift5 to your BI tool (in this case PowerBI) and create the charts... This is an example of how the powerbi dashbaord should look like. ![Dashboard](./dashboard.png) 

8. **CI/CD**: The project includes GitHub Actions workflows for continuous integration and deployment. Ensure that your code passes the CI checks before merging changes.

## Choice of Tools and Technologies

- **Apache Airflow**: Used for orchestrating the data pipeline, scheduling tasks, and managing dependencies.
- **AWS (Amazon Web Services)**: Chosen for its robust cloud infrastructure and wide range of services suitable for building scalable data platforms.
- **Amazon S3**: Used as the primary data lake for storing raw data files from various sources.
- **Amazon Redshift**: Chosen as the data warehouse solution for its scalability, performance, and integration with other AWS services.
- **DBT (Data Build Tool)**: Utilized for transforming and modeling data within the Redshift data warehouse.
- **Terraform**: Employed for Infrastructure as Code (IaC) to provision and manage AWS resources.
- **Docker**: Used for containerizing the application and ensuring consistent environments across development and production.
- **Python**: The primary programming language for writing data ingestion scripts, Airflow DAGs, and the Streamlit application.
- **PowerBI**: Selected for building an interactive dashboard for stakeholders.
- **Github Actions**: Used for implementing CI/CD pipelines to automate testing and deployment processes.

## Key Features

- **Data Ingestion**: Automated ingestion of data from multiple sources, including S3 buckets, postgres transactional database and Google Sheets.
- **Data Transformation**: Use of DBT for transforming raw data into structured formats suitable for analysis.
- **Scalability**: Leveraging AWS services to ensure the platform can scale with increasing data volumes and user demands.
- **Monitoring and Logging**: Integration with Airflow's monitoring capabilities to track the status of data pipeline tasks and log any errors for troubleshooting.
- **Incremental Data Loads**: Support for incremental data loading to optimize performance and reduce processing time.
- **Idempotent Operations**: Ensuring that data ingestion and transformation processes can be safely re-run without causing data duplication or inconsistencies.
- **Retries and alerting**: Built-in retry mechanisms and alerting for failed tasks to ensure reliability and prompt issue resolution.
- **Data Quality Checks**: Implementation of data quality checks to validate the integrity and accuracy of ingested and transformed data.
- **Custom Docker Image**: Creation of a custom Docker image to encapsulate all codes, dependencies, and configurations required for the data platform.
- **Infrastructure as Code**: Use of Terraform to manage and provision AWS resources, ensuring reproducibility and version control of infrastructure.
- **PowerBI Dashboard**: Development of a dashaboard  providing an interactive interface for stakeholders to explore the data.
- **CI/CD Pipelines**: Implementation of CI/CD pipelines using GitHub Actions to automate testing and deployment processes.

## Further Work

- **Advanced Analytics**: Implement advanced analytics and machine learning models on top of the data platform to derive deeper insights into customer experience.
- **Enhanced Data Visualization**: Integrate with BI tools for enhanced data visualization and reporting capabilities.
- **Cost Optimization**: Continuously monitor and optimize the cost of AWS resources used in the platform.
- **Security Enhancements**: Implement additional security measures, such as data encryption and access controls, to protect sensitive customer data.
