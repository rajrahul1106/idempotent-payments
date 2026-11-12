FROM python:3.13.2-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

WORKDIR /srv

RUN apt-get update \
 && apt-get install -y --no-install-recommends gcc libpq-dev curl \
 && rm -rf /var/lib/apt/lists/*

# Every package from the lock, transitive ones included, and nothing else:
# --no-deps stops pip resolving anything the lock does not name, and
# pip check fails the build if the lock is incomplete.
COPY requirements.lock .
RUN pip install --no-cache-dir --no-deps -r requirements.lock && pip check

COPY . .

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
