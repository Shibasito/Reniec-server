FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir pika==1.3.2 matplotlib==3.9.2
COPY client_load_test.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "client_load_test.py"]
