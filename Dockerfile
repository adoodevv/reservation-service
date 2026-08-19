# Node 22.18+ strips TypeScript types natively, so the service runs straight
# from src/ with no build step and no compiled output to keep in sync.
FROM node:22-alpine

WORKDIR /app

# Dependencies first: this layer is cached until the lockfile actually changes,
# so an edit to src/ does not reinstall the tree.
COPY package.json package-lock.json ./
# Production install. The dev dependencies are typescript and vitest, neither of
# which the runtime needs -- type stripping is a Node feature, not a tsc call.
RUN npm ci --omit=dev

COPY . .

# The base image ships an unprivileged `node` user; nothing here needs root.
USER node

EXPOSE 3000

# src/index.ts migrates before it listens, so a fresh database needs no
# separate step. See the comment there for why that is a single-replica
# convenience rather than a deployment pattern.
CMD ["node", "src/index.ts"]
