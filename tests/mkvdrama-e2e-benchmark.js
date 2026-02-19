#!/usr/bin/env node

import 'dotenv/config';
import {
  PROVIDER_CASE,
  DIRECT_CASE,
  NO_RESULT_CASE,
  benchmarkProviderCase,
  benchmarkDirectCase,
  benchmarkNoResultCase
} from '../scripts/benchmark-mkvdrama-e2e.js';

function assertCondition(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function run() {
  console.log('Running MKVDrama E2E benchmark checks...');

  const provider = await benchmarkProviderCase(PROVIDER_CASE);
  const direct = await benchmarkDirectCase(DIRECT_CASE);
  const noResult = await benchmarkNoResultCase(NO_RESULT_CASE);

  console.log('\nProvider case:', JSON.stringify(provider, null, 2));
  console.log('\nDirect case:', JSON.stringify(direct, null, 2));
  console.log('\nNo-result case:', JSON.stringify(noResult, null, 2));

  assertCondition(provider.streamsCount > 0, 'Provider case returned zero streams');
  assertCondition(provider.playable === true, 'Provider case did not produce a playable stream');
  assertCondition(provider.statusCode === 206, `Provider case expected HTTP 206, got ${provider.statusCode}`);

  assertCondition(direct.linksCount > 0, 'Direct case returned zero links');
  assertCondition(direct.playable === true, 'Direct case did not produce a playable stream');
  assertCondition(direct.statusCode === 206, `Direct case expected HTTP 206, got ${direct.statusCode}`);

  assertCondition(noResult.resultsCount === 0, `No-result case unexpectedly returned ${noResult.resultsCount} results`);
  assertCondition(noResult.msToNoResult < 24000, `No-result case exceeded 24s (${noResult.msToNoResult}ms)`);

  console.log('\nMKVDrama E2E benchmark checks passed.');
}

run().then(() => {
  process.exit(0);
}).catch((error) => {
  console.error('\nMKVDrama E2E benchmark checks failed:', error.message);
  process.exit(1);
});
