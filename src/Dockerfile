FROM amsterdam/gob_wheelhouse:3.9-slim-bullseye

WORKDIR /app

RUN apt-get update && apt-get install -y git libpq-dev python3-dev build-essential antlr4
RUN pip install --upgrade pip

COPY requirements.txt /app/
RUN LIBGDAL_VERSION=$(gdal-config --version) pip install --no-cache-dir \
	--find-links /opt/wheelhouse --requirement requirements.txt
RUN rm requirements.txt

# Wheelhouse cleanup.
RUN rm -rf /opt/wheelhouse

COPY exporttodatabricks exporttodatabricks

CMD ["python", "-m", "exporttodatabricks"]