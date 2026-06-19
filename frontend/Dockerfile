# Build stage
FROM node:20 AS build-stage
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build

# Production stage
FROM nginx:stable-alpine
COPY --from=build-stage /app/dist /usr/share/nginx/html
# Create rules.html as a copy of index.html for backward compatibility
RUN cp /usr/share/nginx/html/index.html /usr/share/nginx/html/rules.html
# Use .template for envsubst support
COPY nginx.conf /etc/nginx/templates/default.conf.template
ENV NGINX_PORT=80
ENV BACKEND_URL=http://app:8080
EXPOSE ${NGINX_PORT}
CMD ["nginx", "-g", "daemon off;"]
