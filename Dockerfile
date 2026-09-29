# Image for the dashboard: `docker compose up` serves it on http://127.0.0.1:3000.
FROM node:24-alpine AS build
WORKDIR /app
COPY package.json package-lock.json tsconfig.base.json ./
COPY packages/core/package.json packages/core/
COPY packages/supervisor/package.json packages/supervisor/
COPY packages/dashboard/package.json packages/dashboard/
COPY bench/package.json bench/
RUN npm ci
COPY packages packages
RUN npm run build

FROM node:24-alpine
WORKDIR /app
ENV NODE_ENV=production
COPY package.json package-lock.json ./
COPY packages/core/package.json packages/core/
COPY packages/supervisor/package.json packages/supervisor/
COPY packages/dashboard/package.json packages/dashboard/
COPY bench/package.json bench/
RUN npm ci --omit=dev -w @meridian/dashboard && npm cache clean --force
COPY --from=build /app/packages/core/dist packages/core/dist
COPY --from=build /app/packages/supervisor/dist packages/supervisor/dist
COPY --from=build /app/packages/dashboard/dist packages/dashboard/dist
COPY packages/dashboard/ui packages/dashboard/ui
USER node
EXPOSE 3000
CMD ["node", "packages/dashboard/dist/cli.js", "--host", "0.0.0.0", "--port", "3000"]
