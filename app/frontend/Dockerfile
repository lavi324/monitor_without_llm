FROM node:18-alpine AS builder

WORKDIR /app

FROM nginx:alpine

COPY index.html /usr/share/nginx/html/

COPY ta9_logo.png /usr/share/nginx/html/

COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
