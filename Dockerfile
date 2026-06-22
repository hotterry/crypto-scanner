FROM python:3.11-slim
WORKDIR /app
COPY . .
EXPOSE 9878
CMD ["python", "comboserver.py"]
