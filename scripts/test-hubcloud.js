#!/usr/bin/env node

import { resolveHttpStreamUrl } from '../lib/http-streams/resolvers/http-resolver.js';
import { processExtractorLinkWithAwait } from '../lib/http-streams/providers/4khdhub/extraction.js';

function printUsage() {
  console.log('Usage: node scripts/test-hubcloud.js <url> [resolver|extractor]');
  console.log('  resolver  - simulate click: resolve via http-resolver (default)');
  console.log('  extractor - direct HubCloud extraction (processExtractorLinkWithAwait)');
}

const [url, modeArg] = process.argv.slice(2);
const mode = (modeArg || 'resolver').toLowerCase();

if (!url) {
  printUsage();
  process.exit(1);
}

if (!['resolver', 'extractor'].includes(mode)) {
  console.log(`Unknown mode: ${mode}`);
  printUsage();
  process.exit(1);
}

console.log(`[test-hubcloud] mode=${mode} url=${url}`);

try {
  if (mode === 'extractor') {
    const results = await processExtractorLinkWithAwait(url, 1);
    const count = Array.isArray(results) ? results.length : 0;
    console.log(`[test-hubcloud] extractor results: ${count}`);
    if (count > 0) {
      console.log('[test-hubcloud] sample:', results.slice(0, 3));
    }
  } else {
    const resolved = await resolveHttpStreamUrl(url);
    console.log('[test-hubcloud] resolver result:', resolved);
  }
} catch (err) {
  console.error('[test-hubcloud] error:', err?.message || err);
  process.exit(1);
}
