
FROM python:3.14-slim-trixie
LABEL maintainer="genuchten@yahoo.com"

RUN apt-get update && apt-get install --yes git \
    && rm -rf /var/lib/apt/lists/*

ENV ROOTPATH=/
ENV POSTGRES_HOST=host.docker.internal
ENV POSTGRES_PORT=5432
ENV POSTGRES_DB=postgres
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=*****

WORKDIR /app

# Install package metadata first for better layer caching
COPY pyproject.toml README.md ./

# Copy source
COPY src ./src

# Install the package
RUN pip install --no-cache-dir .

EXPOSE 8000

CMD ["uvicorn", "csvw_api.main:app", "--reload", "--host", "0.0.0.0", "--port", "8000"]
