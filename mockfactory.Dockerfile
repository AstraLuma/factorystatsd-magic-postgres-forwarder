FROM docker.io/library/python:3-slim
WORKDIR /app
COPY *.json mock-factory.py /app
CMD ["/app/mock-factory.py"]
