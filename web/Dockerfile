FROM node:22-bookworm-slim AS build
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
ARG VITE_API_URL=
ARG VITE_MEDIA_URL=
ENV VITE_API_URL=$VITE_API_URL
ENV VITE_MEDIA_URL=$VITE_MEDIA_URL
RUN npm run build

FROM nginx:1.29-alpine
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
