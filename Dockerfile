# Use the official Python image as the base
FROM python:3.10-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory in the container
WORKDIR /app

# Install necessary tools for downloading the certificate
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Create the directory for PostgreSQL certificates
RUN mkdir -p /root/.postgresql

# Download the root.crt file
RUN curl -o /root/.postgresql/root.crt "https://cockroachlabs.cloud/clusters/8db7f8f9-1f7a-436f-a784-4efcd8400aff/cert"

# Copy requirements.txt and install dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . /app/

# Expose the intended port
EXPOSE 5500

# Run the FastAPI application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5500"]
