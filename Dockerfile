FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080

WORKDIR /app

RUN addgroup --system app && adduser --system --ingroup app app

COPY requirements-api.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements-api.txt

COPY api ./api
COPY domain ./domain
COPY services ./services
COPY utils ./utils
COPY data/ontology ./data/ontology
COPY data/reference ./data/reference
COPY config.py ./config.py

RUN chown -R app:app /app
USER app

EXPOSE 8080
CMD ["sh", "-c", "exec uvicorn api.app:app --host 0.0.0.0 --port ${PORT}"]
