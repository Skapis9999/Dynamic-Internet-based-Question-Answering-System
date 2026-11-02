# One image for every Python service; the k8s Deployment picks the entrypoint.
FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services ./services

# Default: API. Workers override with e.g. ["python", "-m", "services.fetcher.main"].
CMD ["uvicorn", "services.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
