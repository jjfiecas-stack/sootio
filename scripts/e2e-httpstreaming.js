#!/usr/bin/env node

import 'dotenv/config';
import addonInterface from '../addon.js';
import { resolveHttpStreamUrl } from '../lib/http-streams/resolvers/http-resolver.js';
import { validateSeekableUrl } from '../lib/http-streams/utils/validation.js';

function printUsage() {
  console.log('Usage: node scripts/e2e-httpstreaming.js <config-url-or-json> <imdbId> <season> <episode>');
}

function extractConfig(input) {
  if (!input) return null;

  // If input looks like raw JSON, parse directly
  if (input.trim().startsWith('{')) {
    return JSON.parse(input);
  }

  // Try to decode URL-encoded JSON from a Stremio config URL
  const decoded = decodeURIComponent(input);
  const start = decoded.indexOf('{');
  const end = decoded.lastIndexOf('}');
  if (start !== -1 && end !== -1 && end > start) {
    const jsonStr = decoded.slice(start, end + 1);
    return JSON.parse(jsonStr);
  }

  throw new Error('Could not extract config JSON from input');
}

function groupByProvider(streams) {
  const counts = new Map();
  for (const s of streams || []) {
    const group = s?.behaviorHints?.bingeGroup || 'unknown';
    counts.set(group, (counts.get(group) || 0) + 1);
  }
  return counts;
}

function pickStream(streams) {
  if (!streams?.length) return null;
  const preferred = streams.find(s => (s?.behaviorHints?.bingeGroup || '').includes('4khdhub'));
  return preferred || streams[0];
}

function extractResolverUrl(streamUrl) {
  if (!streamUrl) return null;
  // Handle proxy wrapper URLs (mediaflow): ?d=<encoded original url>
  try {
    const proxyUrl = new URL(streamUrl);
    const proxyTarget = proxyUrl.searchParams.get('d');
    if (proxyTarget) {
      const decodedProxyTarget = decodeURIComponent(proxyTarget);
      // If proxy target is a resolve URL, extract from it
      const nested = extractResolverUrl(decodedProxyTarget);
      return nested || decodedProxyTarget;
    }
  } catch {
    // Fall through to direct resolver extraction
  }
  try {
    const u = new URL(streamUrl);
    const marker = '/resolve/httpstreaming/';
    const idx = u.pathname.indexOf(marker);
    if (idx === -1) return null;
    const encoded = u.pathname.slice(idx + marker.length);
    return decodeURIComponent(encoded);
  } catch {
    return null;
  }
}

const [configInput, imdbId, season, episode] = process.argv.slice(2);
if (!configInput || !imdbId || !season || !episode) {
  printUsage();
  process.exit(1);
}

const config = extractConfig(configInput);
config.host = config.host || 'http://localhost:7000';
config.clientIp = config.clientIp || '127.0.0.1';

const seriesId = `${imdbId}:${season}:${episode}`;

console.log(`[e2e] Fetching streams for ${seriesId}`);
const result = await addonInterface.get('stream', 'series', seriesId, {}, config);
const streams = result?.streams || [];
console.log(`[e2e] Total streams: ${streams.length}`);

const counts = groupByProvider(streams);
console.log('[e2e] Streams by provider group:');
for (const [group, count] of counts.entries()) {
  console.log(`  - ${group}: ${count}`);
}

const chosen = pickStream(streams);
if (!chosen) {
  console.log('[e2e] No streams returned');
  process.exit(0);
}

console.log(`[e2e] Chosen stream group=${chosen.behaviorHints?.bingeGroup || 'unknown'}`);
console.log(`[e2e] Chosen stream title: ${chosen.title?.split('\n')[0] || 'unknown'}`);

const originalUrl = extractResolverUrl(chosen.url);
if (!originalUrl) {
  console.log('[e2e] Chosen stream is not a httpstreaming resolver URL; cannot resolve');
  console.log('[e2e] URL:', chosen.url);
  process.exit(0);
}

console.log('[e2e] Resolving httpstreaming URL...');
const resolved = await resolveHttpStreamUrl(originalUrl);
console.log('[e2e] Resolved URL:', resolved || 'null');

if (resolved) {
  console.log('[e2e] Validating seekable (206) support...');
  const validation = await validateSeekableUrl(resolved, { requirePartialContent: true, timeout: 8000 });
  console.log('[e2e] Validation:', {
    isValid: validation.isValid,
    statusCode: validation.statusCode,
    filename: validation.filename || null,
    contentLength: validation.contentLength || null
  });
}
