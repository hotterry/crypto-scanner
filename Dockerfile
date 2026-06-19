FROM python:3.11-slim

WORKDIR /app

COPY comboserver.py .
COPY contract-terminal.html .

EXPOSE 8080

CMD ["python3", "comboserver.py"]
