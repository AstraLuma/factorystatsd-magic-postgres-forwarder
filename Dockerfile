FROM docker.io/library/python:3 as builder
WORKDIR /project
COPY . /project

# Install poetry
RUN pip install poetry

# Build wheels
RUN poetry build --format=wheel
RUN cp dist/* /tmp

# Export locks
RUN poetry export -o /tmp/requirements.txt -f requirements.txt
RUN pip wheel -r /tmp/requirements.txt -w /tmp

# This is the real container
FROM docker.io/library/python:3-slim
COPY --from=builder /tmp /tmp
RUN pip install --no-cache-dir --disable-pip-version-check /tmp/*.whl

CMD ["pg_magic_forwarder"]
