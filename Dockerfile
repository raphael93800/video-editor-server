FROM python:3.11-bullseye

ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive

# Installer FFmpeg, ImageMagick et les dépendances système
RUN apt-get update && apt-get install -y \
    ffmpeg \
    imagemagick \
    fontconfig \
    wget \
    curl \
    libffi-dev \
    libssl-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Configurer ImageMagick pour autoriser les opérations sur les fichiers (désactiver la politique restrictive)
RUN sed -i 's/rights="none" pattern="PDF"/rights="read|write" pattern="PDF"/' /etc/ImageMagick-6/policy.xml && \
    sed -i 's/<policy domain="coder" rights="none" pattern="PS"/<policy domain="coder" rights="read|write" pattern="PS"/' /etc/ImageMagick-6/policy.xml || true && \
    sed -i 's/<policy domain="path" rights="none" pattern="@\*"/<policy domain="path" rights="read|write" pattern="@*"/' /etc/ImageMagick-6/policy.xml || true

# Remplacer complètement la politique ImageMagick pour autoriser tout
RUN echo '<policymap> \
  <policy domain="coder" rights="read|write" pattern="*" /> \
  <policy domain="delegate" rights="read|write" pattern="*" /> \
  <policy domain="filter" rights="read|write" pattern="*" /> \
  <policy domain="path" rights="read|write" pattern="*" /> \
  <policy domain="resource" name="memory" value="2GiB"/> \
  <policy domain="resource" name="map" value="4GiB"/> \
  <policy domain="resource" name="width" value="16KP"/> \
  <policy domain="resource" name="height" value="16KP"/> \
  <policy domain="resource" name="area" value="128MB"/> \
  <policy domain="resource" name="disk" value="8GiB"/> \
</policymap>' > /etc/ImageMagick-6/policy.xml

# Installer la police Montserrat Bold
RUN mkdir -p /usr/share/fonts/truetype/custom && \
    wget -q -O /usr/share/fonts/truetype/custom/Montserrat-Bold.ttf \
    "https://github.com/JulietaUla/Montserrat/raw/master/fonts/ttf/Montserrat-Bold.ttf" && \
    fc-cache -f -v

WORKDIR /app

# Upgrade pip et installer cffi/cryptography en premier
RUN pip install --upgrade pip && \
    pip install --no-cache-dir cffi cryptography

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
