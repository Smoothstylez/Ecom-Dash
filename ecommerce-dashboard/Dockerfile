ARG BUILD_FROM=python:3.11-slim
FROM ${BUILD_FROM}

# HA OS add-on labels
LABEL \
    io.hass.name="E-Commerce Dashboard" \
    io.hass.description="Combined e-commerce analytics dashboard" \
    io.hass.type="addon" \
    io.hass.version="0.2.0"

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ app/
COPY scripts/ scripts/

# Copy seed data (used on first start to populate persistent volume)
COPY data/ data-seed/
COPY storage/ storage-seed/

# Entrypoint
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

EXPOSE 8012

CMD ["/app/run.sh"]
