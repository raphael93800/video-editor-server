FROM python:3.11-slim

# Installer FFmpeg et les dépendances système
RUN apt-get update --fix-missing && apt-get install -y --no-install-recommends \
    ffmpeg \
    fontconfig \
    fonts-open-type \
    wget \
    curl \
    libgl1 \
    libglib2.0-0 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Installer la police Montserrat Bold
RUN mkdir -p /usr/share/fonts/truetype/custom && \
    wget -q -O /usr/share/fonts/truetype/custom/Montserrat-Bold.ttf \
    "https://github.com/JulietaUla/Montserrat/raw/master/fonts/ttf/Montserrat-Bold.ttf" && \
    fc-cache -f -v

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
