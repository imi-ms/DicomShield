FROM python:3.13-slim

WORKDIR /app

# Install dependencies
COPY proxy/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY proxy/. .

# Run the application
CMD ["python", "shield.py"]
