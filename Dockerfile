FROM --platform=linux/amd64 public.ecr.aws/amazonlinux/amazonlinux:2023-minimal AS base

RUN dnf install -y gcc python3.11 python3-devel

ENV VIRTUAL_ENV=/opt/venv
RUN python3.11 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY requirements.txt .
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install -r requirements.txt && \
    python3 -m pip install venv-pack==0.2.0

RUN mkdir /output && venv-pack -o /output/pyspark_ge.tar.gz

FROM scratch AS export
COPY --from=base /output/pyspark_ge.tar.gz /
