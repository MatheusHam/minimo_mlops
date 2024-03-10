# Use a base image with Python installed
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file to the container
COPY requirements.txt .
COPY assets /assets
COPY data_engineering /data_engineering
COPY model_pipeline /model_pipeline
COPY model_serving .

# Install the required Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code to the container
COPY . .

RUN python /data_engineering/prod_data_eng_pipeline.py
RUN python /model_pipeline/prod_model_pipeline.py

# Set the entry point command to run the pipeline
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
