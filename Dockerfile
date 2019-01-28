FROM continuumio/miniconda3
COPY . AutonoTrader/
WORKDIR AutonoTrader
CMD setup.sh
