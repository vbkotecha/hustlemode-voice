FROM python:3.13-slim
WORKDIR /app
COPY agentcourt/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY agentcourt/main.py .
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "${PORT:-8080}"]
