FROM python:3.11-slim

# Home Assistant addon labels
LABEL \
    io.hass.name="E-Commerce Dashboard" \
    io.hass.description="Combined e-commerce analytics dashboard" \
    io.hass.type="addon" \
    io.hass.version="0.3.0"

# Prevent python from writing pyc files and buffering stdout
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies (optional but recommended for many Python libs)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (better Docker cache)
COPY requirements.txt .

# Upgrade pip and install dependencies
RUN python -m pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ app/
COPY scripts/ scripts/

# Copy seed data (used on first start)
COPY data/ data-seed/
COPY storage/ storage-seed/

# Entrypoint
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

# Create directories that will be mounted by Home Assistant
RUN mkdir -p /config/dashboard_combined/data \
    && mkdir -p /config/dashboard_combined/storage

EXPOSE 8012

CMD ["/app/run.sh"]
