# Use an official Node.js Alpine image
FROM node:20-alpine

# Set working directory inside container
WORKDIR /app

# Install git, build tools, and Chromium for Puppeteer stealth browser (BTDigg)
RUN apk add --no-cache \
    git \
    curl \
    python3 \
    make \
    g++ \
    sqlite \
    sqlite-dev \
    chromium \
    nss \
    freetype \
    harfbuzz \
    ca-certificates \
    ttf-freefont

# Tell Puppeteer to skip downloading its own Chromium
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true
ENV CHROMIUM_PATH=/usr/bin/chromium-browser

# Copy only dependency files first
COPY package*.json ./
# Note: If you use pnpm-lock.yaml, ensure it exists in your fork root
COPY pnpm-lock.yaml ./ 

# Install pnpm and then dependencies
RUN npm install -g pnpm@9 && pnpm install --no-frozen-lockfile

# Copy rest of the project files
COPY . .

# Expose app port
EXPOSE 6907

# CHANGE: Run the node command directly to allow Docker Compose overrides
CMD ["node", "--max-old-space-size=12000", "--expose-gc", "server.js"]
