# syntax=docker/dockerfile:1

# ---- Stage 1: install production dependencies ----
# Alpine (musl) carries none of the Debian/glibc OS packages that the image
# scanner flagged (glibc, perl, pam, systemd, ncurses, gnutls, GNU tar, bash,
# coreutils, …), so moving off bookworm clears that entire class of CVEs.
# Node 20 reached end-of-life (no more security updates) in April 2026, so we
# move to the supported LTS line, Node 22.
FROM node:22-alpine AS deps

WORKDIR /app

COPY package.json ./

# The "overrides" block in package.json forces the patched tar-fs (>=2.1.5)
# through dockerode's pinned "~2.0.1" range, clearing the tar-fs CVEs while
# staying API-compatible.
RUN npm install --omit=dev --no-audit --no-fund

# ---- Stage 2: runtime ----
FROM node:22-alpine

WORKDIR /app

# Pull the latest Alpine security patches, then remove npm. The app starts with
# `node`, not `npm`, so npm is dead weight at runtime — and its bundled
# packages (tar, glob, minimatch, cross-spawn, brace-expansion) are exactly the
# ones the scanner flagged. Dropping npm removes them from the final image.
RUN apk upgrade --no-cache \
    && rm -rf /usr/local/lib/node_modules/npm \
              /usr/local/bin/npm \
              /usr/local/bin/npx

# Only the resolved production node_modules and the app sources are carried into
# the final image — no build tooling.
COPY --from=deps /app/node_modules ./node_modules
COPY package.json ./
COPY server.js ./

EXPOSE 3001

# Equivalent to the previous `npm start` ("node server.js"), without needing npm.
CMD ["node", "server.js"]
