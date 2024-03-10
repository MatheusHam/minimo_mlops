# Use a base image with Python installed
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file to the container
COPY requirements.txt .

# Install the required Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code to the container
COPY . .

# Set the entry point command to run the pipeline
CMD ["python", "your_pipeline_script.py"]