# =============================================================================
# Stage 1: Build
# =============================================================================
FROM node:22-slim@sha256:4f77a690f2f8946ab16fe1e791a3ac0667ae1c3575c3e4d0d4589e9ed5bfaf3d AS builder

WORKDIR /app

# Copy workspace root
COPY package.json tsconfig.base.json ./

# Copy shared package
COPY packages/shared/package.json packages/shared/tsconfig.json ./packages/shared/
COPY packages/shared/src ./packages/shared/src

# Copy worker package
COPY packages/worker/package.json packages/worker/tsconfig.json ./packages/worker/
COPY packages/worker/src ./packages/worker/src

# Install dependencies and build
RUN npm install --workspace=packages/shared --workspace=packages/worker
RUN npm run build -w packages/shared
RUN npm run build -w packages/worker

# =============================================================================
# Stage 2: Production
# =============================================================================
FROM node:22-slim@sha256:4f77a690f2f8946ab16fe1e791a3ac0667ae1c3575c3e4d0d4589e9ed5bfaf3d AS production

# Pin Claude CLI version
ARG CLAUDE_CLI_VERSION=2.1.77

# Install system tools + tini for proper PID 1
RUN apt-get update && \
    apt-get install -y --no-install-recommends git curl jq tini ca-certificates bubblewrap && \
    rm -rf /var/lib/apt/lists/*

# Install Claude Code CLI with pinned version
RUN npm install -g @anthropic-ai/claude-code@${CLAUDE_CLI_VERSION}

WORKDIR /app

# Create non-root user
RUN groupadd -r dac && useradd -r -g dac -m -d /home/dac -s /bin/false dac

# Copy built artifacts from builder
COPY --from=builder /app/package.json ./
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/packages/shared/dist ./packages/shared/dist
COPY --from=builder /app/packages/shared/package.json ./packages/shared/
COPY --from=builder /app/packages/shared/node_modules ./packages/shared/node_modules
COPY --from=builder /app/packages/worker/dist ./packages/worker/dist
COPY --from=builder /app/packages/worker/package.json ./packages/worker/
COPY --from=builder /app/packages/worker/node_modules ./packages/worker/node_modules

# Pre-create claude config for dac user
RUN mkdir -p /home/dac/.claude && \
    echo '{"hasCompletedOnboarding":true}' > /home/dac/.claude.json && \
    chown -R dac:dac /home/dac

# Temp directory for execution sandboxes
RUN mkdir -p /tmp/dac-exec && chown dac:dac /tmp/dac-exec

USER dac
ENV HOME=/home/dac

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD node -e "process.exit(0)"

# No EXPOSE — outbound-only connections
ENTRYPOINT ["tini", "--"]
CMD ["node", "packages/worker/dist/index.js"]
