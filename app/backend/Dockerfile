FROM node:20-bookworm-slim

WORKDIR /app

# Pull latest security fixes from Debian repos at build time.
RUN apt-get update \
	&& apt-get upgrade -y \
	&& rm -rf /var/lib/apt/lists/*

COPY package.json ./

RUN npm install --omit=dev

COPY server.js ./

EXPOSE 3001

CMD ["npm", "start"]
