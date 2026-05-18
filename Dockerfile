FROM node:20-alpine

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev --no-audit --no-fund && npm cache clean --force

COPY server.js ./
COPY src ./src

ENV NODE_ENV=production \
    HOST=0.0.0.0 \
    PORT=3000 \
    TOKEN_FILE=/data/token.json

EXPOSE 3000

VOLUME ["/data"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://127.0.0.1:3000/v1/models > /dev/null || exit 1

CMD ["node", "server.js"]
