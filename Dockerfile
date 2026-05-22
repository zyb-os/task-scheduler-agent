FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV ORCHESTRATOR_URL=http://host.docker.internal:8000
CMD ["python", "main.py"]
