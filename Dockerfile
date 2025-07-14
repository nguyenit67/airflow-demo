FROM apache/airflow:3.0.2

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /opt/airflow

# Copy requirements file
COPY requirements.txt ./

# Switch to root user for system package installation
USER root

# Install dependencies in system Python
RUN uv pip install --system -r requirements.txt

# Switch back to airflow user
USER airflow

# Copy the rest of your project
COPY . .
