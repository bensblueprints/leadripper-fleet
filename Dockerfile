FROM node:20-alpine

RUN apk add --no-cache python3 make g++ sqlite

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev

COPY server.js db.js license-hashes.json ./
COPY public ./public

ENV DATA_DIR=/data
ENV PORT=3000
VOLUME ["/data"]

EXPOSE 3000

CMD ["node", "server.js"]
