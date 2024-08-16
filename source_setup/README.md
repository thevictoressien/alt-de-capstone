# Source Setup

This directory contains essential resources for initializing the database that serves as the source system for this project.

## Contents

- `init.sql`: SQL script for creating and populating the initial database schema

## Automated Setup

The `init.sql` script is automatically executed when running Docker Compose, thanks to the volume mapping in the `docker-compose.yml` file.

## Usage

1. Ensure Docker and Docker Compose are installed on your system
2. Navigate to the project root directory
3. Run `docker-compose up` to start the services and initialize the database
4. The source database will be automatically set up and populated with initial data gotten from the 
    csv files in the `../data` directory

This automated setup provides the foundation for the data pipeline and analysis tasks in this project, requiring minimal manual intervention.
