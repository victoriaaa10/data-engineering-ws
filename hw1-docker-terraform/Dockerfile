# Use a lightweight Python 3.13 base
FROM python:3.13-slim

# Install uv from the official binary
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set working directory
WORKDIR /app

# Enable bytecode compilation for faster execution
ENV UV_COMPILE_BYTECODE=1

# Cache dependencies: Copy lockfiles before code
# We copy .python-version too to ensure uv uses the correct toolchain
COPY pyproject.toml uv.lock .python-version ./

# Install dependencies without the project code first (better for caching)
RUN uv sync --frozen --no-install-project

# Copy the entire src directory
COPY src/ ./src/

# Final sync to install the project itself
RUN uv sync --frozen

# Define the entrypoint using the modular path
ENTRYPOINT ["uv", "run", "python", "src/pipeline/main.py"]