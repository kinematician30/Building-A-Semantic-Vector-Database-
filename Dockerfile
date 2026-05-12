# Start from a base image: using the spark image
FROM spark:python3-java17

# permanent enviroment variable to add and execute spark
ENV PATH=$PATH:/opt/spark/bin

# Creating and setting the working directory to /app
WORKDIR /app

# Switching to 'root' user for permission to instal; softwares
USER root

# THIS IS TO AVOID INSTALLING CUDA-DEPENDENT VERSIONS OF PYTORCH, WHICH CAN CAUSE ISSUES IN A CPU-ONLY ENVIRONMENT
RUN pip install torch --index-url https://download.pytorch.org/whl/cpu

# Copy only the requirements first
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your scripts
COPY ingestion.py transformation.py load_gold.py ./

# Create the data structure
# We only copy the 'bronze' folder as you requested
# COPY data/bronze/ ./data/bronze/

# Create empty folders for silver and gold (to be populated during run)
RUN mkdir -p data/bronze data/silver data/gold

# Set environment variables for Spark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# make the container listen on the host os port
EXPOSE 4040

# Switch back to the default user
USER spark