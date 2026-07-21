FROM node:24.15.0-alpine AS dependencies

WORKDIR /app

COPY apps/web/package.json apps/web/package-lock.json ./
RUN npm ci

FROM dependencies AS build

COPY apps/web ./

ARG PUBLIC_API_BASE_URL=http://127.0.0.1:8020
ENV PUBLIC_API_BASE_URL=${PUBLIC_API_BASE_URL}
RUN npm run build

FROM node:24.15.0-alpine AS runtime

ENV HOST=0.0.0.0 \
    PORT=4321 \
    NODE_ENV=production

WORKDIR /app

COPY apps/web/package.json apps/web/package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts \
    && npm cache clean --force

COPY --from=build --chown=node:node /app/dist ./dist

USER node

EXPOSE 4321
CMD ["node", "./dist/server/entry.mjs"]
