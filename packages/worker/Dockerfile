FROM node:22-slim

# Install system tools agents might need
RUN apt-get update && \
    apt-get install -y git curl jq && \
    rm -rf /var/lib/apt/lists/*

# Install Claude Code CLI globally
RUN npm install -g @anthropic-ai/claude-code

WORKDIR /app

# Copy workspace root
COPY package.json ./
COPY tsconfig.base.json ./

# Copy shared package
COPY packages/shared/package.json ./packages/shared/
COPY packages/shared/tsconfig.json ./packages/shared/
COPY packages/shared/src ./packages/shared/src

# Copy worker package
COPY packages/worker/package.json ./packages/worker/
COPY packages/worker/tsconfig.json ./packages/worker/
COPY packages/worker/src ./packages/worker/src

# Install dependencies
RUN npm install --workspace=packages/shared --workspace=packages/worker

# Build shared first, then worker
RUN npm run build -w packages/shared
RUN npm run build -w packages/worker

# Pre-create claude config to skip onboarding
RUN echo '{"hasCompletedOnboarding":true}' > /root/.claude.json

# No EXPOSE — outbound-only connections
CMD ["node", "packages/worker/dist/index.js"]
