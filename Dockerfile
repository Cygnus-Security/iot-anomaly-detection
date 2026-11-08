FROM python:3.12.8-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY pyproject.toml ./

# Install the dependency layer before application source so routine code edits
# do not redownload Streamlit/Pandas during every rebuild.
RUN mkdir -p src && pip install --no-cache-dir ".[dev]"

ENV PYTHONPATH=/app

COPY .streamlit ./.streamlit
COPY src ./src
COPY tests ./tests

CMD ["python", "-m", "src.simulator.main"]
