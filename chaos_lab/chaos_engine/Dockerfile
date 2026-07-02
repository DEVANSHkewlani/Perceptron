FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --default-timeout=1000 -r requirements.txt
COPY . .
EXPOSE 9091
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "9091"]
