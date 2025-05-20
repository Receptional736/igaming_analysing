FROM python:3.11-slim

WORKDIR /app
COPY . .

# install Python deps first (includes the playwright *Python package*)
RUN pip install --no-cache-dir -r requirements.txt

# now install the actual browser binaries + system libs
RUN playwright install --with-deps chromium

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
