FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create tables before starting the app
CMD ["sh", "-c", "python create_tables.py && python app.py"]
