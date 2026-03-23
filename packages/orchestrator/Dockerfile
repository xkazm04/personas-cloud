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

# Copy orchestrator package
COPY packages/orchestrator/package.json packages/orchestrator/tsconfig.json ./packages/orchestrator/
COPY packages/orchestrator/src ./packages/orchestrator/src

# Install dependencies and build
RUN npm install --workspace=packages/shared --workspace=packages/orchestrator
RUN npm run build -w packages/shared
RUN npm run build -w packages/orchestrator

# =============================================================================
# Stage 2: Production
# =============================================================================
FROM node:22-slim@sha256:4f77a690f2f8946ab16fe1e791a3ac0667ae1c3575c3e4d0d4589e9ed5bfaf3d AS production

# Install tini for proper PID 1
RUN apt-get update && \
    apt-get install -y --no-install-recommends tini && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Create non-root user
RUN groupadd -r dac && useradd -r -g dac -m -d /home/dac -s /bin/false dac

# Copy built artifacts from builder
COPY --from=builder /app/package.json ./
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/packages/shared/dist ./packages/shared/dist
COPY --from=builder /app/packages/shared/package.json ./packages/shared/
COPY --from=builder /app/packages/shared/node_modules ./packages/shared/node_modules
COPY --from=builder /app/packages/orchestrator/dist ./packages/orchestrator/dist
COPY --from=builder /app/packages/orchestrator/package.json ./packages/orchestrator/
COPY --from=builder /app/packages/orchestrator/node_modules ./packages/orchestrator/node_modules

# Data directory (writable by dac user for SQLite)
RUN mkdir -p /app/data && chown -R dac:dac /app/data

USER dac

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD node -e "const h=require('http');h.get('http://localhost:3001/health',r=>{process.exit(r.statusCode===200?0:1)}).on('error',()=>process.exit(1))"

EXPOSE 8443 3001

ENTRYPOINT ["tini", "--"]
CMD ["node", "packages/orchestrator/dist/index.js"]
