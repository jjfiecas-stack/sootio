# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Sootio is a Stremio addon that aggregates streaming links from multiple sources:
- **7 Debrid providers**: Real-Debrid, All-Debrid, TorBox, Premiumize, OffCloud, Debrid-Link, Debrider.app
- **14+ torrent scrapers**: Jackett, Zilean, 1337x, BTDigg, MagnetDL, Torrentio, Comet, etc.
- **HTTP streaming providers**: 4KHDHub, UHDMovies, MKVDrama, NetflixMirror, etc.
- **Usenet support**: Newznab indexers + SABnzbd with progressive streaming

Built with Node.js 20, ESM modules, Express, and the Stremio Addon SDK.

## Common Commands

```bash
# Install dependencies (pnpm required)
pnpm install

# Production with multi-worker clustering
npm start                    # or: npm run start

# Development with auto-reload
npm run dev

# Single worker mode (debugging)
npm run standalone          # or: npm run standalone:dev for debug logs

# Run tests
npm test

# Run single test file
node --max-old-space-size=2048 --expose-gc node_modules/.bin/jest tests/mkvdrama.test.js

# Docker
docker-compose up -d --build
docker-compose logs -f
```

## Architecture

### Entry Points
- `server.js` - Express server, route handlers, Stremio SDK integration (~1400 lines)
- `cluster.js` - Multi-worker process management with crash loop protection
- `addon.js` - Stremio addon builder, catalog and stream handlers

### Core Flow
1. **Request** → `addon.js` defineStreamHandler
2. **Orchestration** → `lib/stream-provider.js` coordinates all sources in parallel
3. **Scraping** → `lib/scrapers/` fetches torrent metadata from enabled sources
4. **Cache Check** → `lib/common/debrid-cache-processor.js` checks debrid availability
5. **Formatting** → `lib/stream-provider/formatters/` formats streams for Stremio

### Key Directories
```
lib/
├── scrapers/              # Torrent scrapers by category
│   ├── public-trackers/   # 1337x, btdig, magnetdl, etc.
│   ├── torznab/           # Jackett, Zilean, Bitmagnet
│   ├── stremio-addons/    # Torrentio, Comet, StremThru bridges
│   └── specialized/       # Wolfmax4K, BluDV, Snowfl
├── http-streams/          # HTTP streaming providers
│   ├── providers/         # 4khdhub, mkvdrama, netflixmirror, etc.
│   ├── resolvers/         # Link resolution (hubcloud, pixeldrain, etc.)
│   └── utils/             # HTTP helpers, parsing, validation
├── util/                  # Shared utilities
│   ├── cache-store.js     # SQLite cache backend selector
│   ├── postgres-cache.js  # Postgres cache for multi-instance
│   ├── rd-rate-limit.js   # Real-Debrid rate limiter
│   ├── ad-rate-limit.js   # All-Debrid rate limiter
│   ├── proxy-manager.js   # SOCKS5/HTTP proxy handling
│   └── cinemeta.js        # IMDB metadata fetching
├── stream-provider/       # Stream orchestration modules
│   ├── caching/           # Background refresh, deduplication
│   ├── formatters/        # Stream name/description formatting
│   └── utils/             # Filtering, sorting utilities
└── [provider].js          # Debrid provider implementations
```

### Debrid Provider Pattern
Each debrid provider (`lib/real-debrid.js`, `lib/all-debrid.js`, etc.) implements:
- `checkCachedTorrents(apiKey, magnets)` - Check cache availability
- `getDownloadUrl(apiKey, magnet, fileIdx)` - Get streaming URL
- Personal cloud/downloads listing

### Adding a New Scraper
1. Create file in `lib/scrapers/[category]/` following existing patterns
2. Export `scrapeTorrents(imdbId, type, title, year, season, episode)` function
3. Register in `lib/scrapers/index.js`
4. Add env vars in `.env.example` with `[NAME]_ENABLED`, `[NAME]_URL`, etc.

### Adding a New HTTP Stream Provider
1. Create directory in `lib/http-streams/providers/[name]/`
2. Implement `search.js` and `streams.js`
3. Register in `lib/http-streams.js` exports
4. Add to `lib/stream-provider.js` HTTP streaming section

## Configuration

All config via `.env` file. Key patterns:
- `[SCRAPER]_ENABLED=true/false` - Enable/disable scrapers
- `[SCRAPER]_URL` - Base URL for scraper
- `[SCRAPER]_LIMIT` - Max results per search
- `RD_*`, `AD_*` - Rate limits for debrid providers
- `DEBRID_HTTP_PROXY` - SOCKS5/HTTP proxy URL
- `SQLITE_CACHE_ENABLED=true` - Enable persistent cache
- `CACHE_BACKEND=sqlite|postgres` - Cache backend selection

## Testing

Tests are in `tests/` using Jest. Most tests are integration tests that hit external APIs.

```bash
# Run all tests
npm test

# Run specific test
npm test -- tests/mkvdrama.test.js

# Run with verbose output
npm test -- --verbose
```

## Important Patterns

### Rate Limiting
Debrid APIs have strict rate limits. Use the rate limiters in `lib/util/rd-rate-limit.js` and `lib/util/ad-rate-limit.js`.

### Proxy Support
Proxy configuration flows through `lib/util/proxy-manager.js`. Per-service proxies can be configured via `DEBRID_PER_SERVICE_PROXIES`.

### Caching Layers
1. **In-memory** - NodeCache for hot data (5000 entries)
2. **SQLite** - Persistent cache (`data/` directory)
3. **Postgres** - Optional shared cache for multi-instance deployments

### ESM Modules
This project uses ES modules (`"type": "module"` in package.json). Use `import`/`export` syntax, not `require()`.
