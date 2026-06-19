FROM python:3.11-slim

WORKDIR /app
COPY comboserver.py contract-terminal.html ./

EXPOSE 8080

CMD ["python", "comboserver.py"]
