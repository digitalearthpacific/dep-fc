
ARG BUILDPLATFORM="linux/amd64"
FROM --platform=${BUILDPLATFORM} ghcr.io/astral-sh/uv:python3.10-bookworm-slim

ENV USE_PYGEOS=0
ENV FC=gfortran-11

#Install os depencdencies
RUN echo 'deb http://deb.debian.org/debian/ unstable main contrib non-free' >> /etc/apt/sources.list
RUN apt-get update && apt-get upgrade -y
RUN apt install -y -t unstable gdal-bin
RUN apt-get install -y git libpq-dev libgdal-dev gdal-bin clang gfortran-11 g++ cmake
RUN apt-get autoclean -y && apt-get autoremove -y && rm -rf /var/lib/{apt,dpkg,cache,log}

ADD src /app
ADD pyproject.toml /app/
ADD setup_fc.sh /app/
WORKDIR /app

#Install app dependencies
RUN uv venv
RUN uv sync -U --compile-bytecode

#Compile FC Fortran
RUN apt remove -y gcc g++
ENV PATH="/app/.venv/bin:$PATH"
RUN sh setup_fc.sh

# Byte Compile FC
RUN uv run python -c "from fc import fractional_cover"
#CMD ["uv", "run", "run.py"]


