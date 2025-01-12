FROM rocker/r-ver:4.2.2

RUN apt-get update
RUN apt-get install -y libcurl4-openssl-dev
RUN apt-get install -y libpq-dev
RUN apt-get install -y libssl-dev

ENV RENV_PATHS_CACHE /renv

COPY ./renv /renv

WORKDIR /code

COPY ./ /code

RUN R -e "renv::restore()"

CMD ["Rscript", "main.R"]

LABEL org.opencontainers.image.source https://github.com/ndrewgele/indicator-calc