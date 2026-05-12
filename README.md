# Building-A-Semantic-Vector-Database-

You are coming in as a Data Infrastructure Engineer for the AI Search Initiative. Your responsibility is architect, and deploy the end-to-end data pipeline required to ingest, transform, and manage the semantic data foundations. You are expected to deliver data-engineering solutions that will help build a scalable AI infrastructure on premise.

## Container Branch - Getting Started

This guide covers how to set up and run the semantic vector database pipeline using Docker containers.

### Prerequisites

- Docker installed and running
- Docker Compose (optional, for multi-container orchestration)
- Access to metadata and review data files

### Setup Instructions

#### 1. Create Data Directory Structure

Before running the container, you need to set up the local data directory:

```bash
# Create the data directory structure - macOS/Linux
mkdir -p data/bronze/
```


#### 2. Download Data Files

Download the following folders from this drive and place them in the created `bronze` directory:

- **metadata folder**: Follow [here](https://drive.google.com/drive/folders/1jmBmGyAS-JJC9mRm0cAARuhuUiZYdl8k?usp=sharing) and place the files in `data/bronze/`
- **review folder**: Follow [here](https://drive.google.com/drive/folders/1qP1c59P9RZS13D73eAr-vTXSHNy7u5zt?usp=sharing) and place the files in `data/bronze/`

Your directory structure should look like:
```
data/
└── bronze/
    ├── metadata/
    │   └── [metadata files from drive]
    └── review/
        └── [review files from drive]
```

### Running the Container

Build and run the Docker container:

![YOUR TURN DEVOPS](https://i.imgflip.com/ark5f5.jpg)


The container will process the data in the bronze folder and generate the silver and gold layers in the `data/` directory on your local machine.

### Pipeline Stages

- **Bronze Layer**: Raw ingested data
- **Silver Layer**: Cleaned and transformed data
- **Gold Layer**: Aggregated and vectorized data ready for AI search applications
