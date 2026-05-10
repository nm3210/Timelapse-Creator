FROM node:18-slim

# Tell the system to use the modern Intel iHD driver
ENV LIBVA_DRIVER_NAME=iHD

WORKDIR /app

# Install standard open-source drivers and FFmpeg directly from main
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    intel-media-va-driver \
    libmfx1 \
    va-driver-all \
    python3 \
    make \
    g++ \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /mnt/photo_import

# Copy package files
COPY package*.json ./

# Install dependencies and rebuild native modules
RUN npm install --production && \
    npm rebuild better-sqlite3 sharp

# Copy application files
COPY . .

# Create directories for snapshots and videos
RUN mkdir -p snapshots videos

# Expose ports
EXPOSE 3001 3002

# Start the server
CMD ["node", "server.js"]