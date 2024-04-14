ARG PYTHON_VERSION=3.11-slim-bullseye

# Python 'run' stage
FROM python:${PYTHON_VERSION} as base

ARG BUILD_ENVIRONMENT=prod
ARG APP_HOME=/app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    BUILD_ENV=${BUILD_ENVIRONMENT} \
    APP_HOME=${APP_HOME}

WORKDIR ${APP_HOME}

# Install system dependencies required for TA-Lib
RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    libpq-dev \
    gettext \
    # Add any other dependencies TA-Lib might need
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Download and install TA-Lib
RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz \
    && tar -xzf ta-lib-0.4.0-src.tar.gz \
    && cd ta-lib/ \
    && wget -O config.guess 'https://git.savannah.gnu.org/gitweb/?p=config.git;a=blob_plain;f=config.guess;hb=HEAD' \
    && wget -O config.sub 'https://git.savannah.gnu.org/gitweb/?p=config.git;a=blob_plain;f=config.sub;hb=HEAD' \
    && ./configure --prefix=/usr \
    && make \
    && make install \
    && cd .. \
    && rm -rf ta-lib-0.4.0-src.tar.gz ta-lib/

# Install Python dependencies
COPY ./requirements /requirements
RUN pip wheel --wheel-dir ${APP_HOME}/wheels -r /requirements/${BUILD_ENVIRONMENT}.txt
#RUN pip install --no-cache-dir --no-index --find-links=/app/wheels/* && rm -rf /app/wheels/
RUN for wheel in /app/wheels/*.whl; do pip install $wheel; done && rm -rf /app/wheels/

COPY . ${APP_HOME}
RUN rm -rf ${APP_HOME}/state


# Setup entrypoint and start scripts if necessary
# COPY ./compose/prod/dataflows/entrypoint /entrypoint
# RUN sed -i 's/\r//g' /entrypoint && chmod +x /entrypoint
COPY ./compose/prod/manage/start /start
RUN sed -i 's/\r//g' /start && chmod +x /start

# ENTRYPOINT ["/entrypoint"]
CMD ["sh", "-c", "/start"]

# For debugging and library installation, override the CMD with a shell
#CMD ["bash"]