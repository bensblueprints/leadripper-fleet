FROM node:20-alpine

RUN apk add --no-cache python3 make g++ sqlite

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev

COPY server.js db.js ghl-sync.js ai-agent.js license-hashes.json ./
COPY public ./public
COPY mass-scrape ./mass-scrape

ENV DATA_DIR=/data
ENV PORT=3000
VOLUME ["/data"]

EXPOSE 3000

CMD ["node", "server.js"]
