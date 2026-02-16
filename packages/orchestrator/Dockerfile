FROM node:22-slim

WORKDIR /app

# Copy workspace root
COPY package.json ./
COPY tsconfig.base.json ./

# Copy shared package
COPY packages/shared/package.json ./packages/shared/
COPY packages/shared/tsconfig.json ./packages/shared/
COPY packages/shared/src ./packages/shared/src

# Copy orchestrator package
COPY packages/orchestrator/package.json ./packages/orchestrator/
COPY packages/orchestrator/tsconfig.json ./packages/orchestrator/
COPY packages/orchestrator/src ./packages/orchestrator/src

# Install dependencies
RUN npm install --workspace=packages/shared --workspace=packages/orchestrator

# Build shared first, then orchestrator
RUN npm run build -w packages/shared
RUN npm run build -w packages/orchestrator

EXPOSE 8443 3001

CMD ["node", "packages/orchestrator/dist/index.js"]
