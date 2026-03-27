
FROM python:3.14-slim-trixie
LABEL maintainer="genuchten@yahoo.com"

RUN apt-get update && apt-get install --yes \
        ca-certificates libexpat1 git \
    && rm -rf /var/lib/apt/lists/*

RUN adduser --uid 1000 --gecos '' --disabled-password sds

ENV ROOTPATH=/
ENV POSTGRES_HOST=host.docker.internal
ENV POSTGRES_PORT=5432
ENV POSTGRES_DB=postgres
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=*****

WORKDIR /home/soildatastreamer

RUN chown --recursive sds:sds .

# initially copy only the requirements files
COPY --chown=sds \
    requirements.txt \
    ./

RUN pip install -U pip && \
    python3 -m pip install \
    -r requirements.txt \
    psycopg2-binary  

COPY --chown=sds . .

WORKDIR /home/soildatastreamer

EXPOSE 8000

USER sds

ENTRYPOINT [ "python3", "-m", "uvicorn", "app.main:app", "--reload", "--host", "0.0.0.0", "--port", "8000" ]
