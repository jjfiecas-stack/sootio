#!/usr/bin/env node

import 'dotenv/config';
import path from 'path';
import { fileURLToPath } from 'url';
import { getMkvDramaStreams } from '../lib/http-streams/providers/mkvdrama/streams.js';
import { loadMkvDramaContent, scrapeMkvDramaSearch } from '../lib/http-streams/providers/mkvdrama/search.js';
import { resolveHttpStreamUrl } from '../lib/http-streams/resolvers/http-resolver.js';
import { validateSeekableUrl } from '../lib/http-streams/utils/validation.js';

const PROVIDER_CASE = {
  name: 'provider-series',
  tmdbId: 'tt0000000',
  type: 'series',
  season: '1',
  episode: '2',
  meta: {
    name: 'The Judge Returns',
    alternativeTitles: [],
    original_title: 'The Judge Returns'
  }
};

const DIRECT_CASE = {
  name: 'direct-post',
  postUrl: 'https://mkvdrama.net/the-judge-returns/',
  season: '1',
  episode: '2'
};

const NO_RESULT_CASE = {
  name: 'no-result-search',
  query: 'Only Murders in the Building'
};

function getHost(url) {
  if (!url) return null;
  try {
    return new URL(url).hostname;
  } catch {
    return null;
  }
}

async function benchmarkProviderCase(testCase) {
  const startedAt = Date.now();
  const streams = await getMkvDramaStreams(
    testCase.tmdbId,
    testCase.type,
    testCase.season,
    testCase.episode,
    { clientIp: '127.0.0.1' },
    testCase.meta
  );
  const streamFetchEndedAt = Date.now();

  const firstStream = (streams || []).find((stream) => typeof stream?.url === 'string') || null;
  let resolvedUrl = null;
  let validation = null;
  let resolveDurationMs = null;

  if (firstStream?.url) {
    const resolveStartedAt = Date.now();
    resolvedUrl = await resolveHttpStreamUrl(firstStream.url);
    resolveDurationMs = Date.now() - resolveStartedAt;
    if (resolvedUrl) {
      validation = await validateSeekableUrl(resolvedUrl, {
        requirePartialContent: true,
        timeout: 8000
      });
    }
  }

  return {
    case: testCase.name,
    streamsCount: streams.length,
    msToStreams: streamFetchEndedAt - startedAt,
    msToResolve: resolveDurationMs,
    msToVideoStartEstimate: resolveDurationMs === null ? null : (streamFetchEndedAt - startedAt) + resolveDurationMs,
    firstStreamTitle: firstStream?.title?.split('\n')[0] || null,
    resolvedHost: getHost(resolvedUrl),
    playable: Boolean(validation?.isValid),
    statusCode: validation?.statusCode ?? null
  };
}

async function benchmarkDirectCase(testCase) {
  const startedAt = Date.now();
  const content = await loadMkvDramaContent(testCase.postUrl, null, {
    season: testCase.season,
    episode: testCase.episode
  });
  const contentFetchEndedAt = Date.now();

  const firstLink = (content.downloadLinks || [])[0] || null;
  let resolvedUrl = null;
  let validation = null;
  let resolveDurationMs = null;

  if (firstLink?.url) {
    const resolveStartedAt = Date.now();
    resolvedUrl = await resolveHttpStreamUrl(firstLink.url);
    resolveDurationMs = Date.now() - resolveStartedAt;
    if (resolvedUrl) {
      validation = await validateSeekableUrl(resolvedUrl, {
        requirePartialContent: true,
        timeout: 8000
      });
    }
  }

  return {
    case: testCase.name,
    linksCount: content.downloadLinks?.length || 0,
    msToContent: contentFetchEndedAt - startedAt,
    msToResolve: resolveDurationMs,
    msToVideoStartEstimate: resolveDurationMs === null ? null : (contentFetchEndedAt - startedAt) + resolveDurationMs,
    resolvedHost: getHost(resolvedUrl),
    playable: Boolean(validation?.isValid),
    statusCode: validation?.statusCode ?? null
  };
}

async function benchmarkNoResultCase(testCase) {
  const startedAt = Date.now();
  const results = await scrapeMkvDramaSearch(testCase.query);
  const endedAt = Date.now();

  return {
    case: testCase.name,
    query: testCase.query,
    resultsCount: results.length,
    msToNoResult: endedAt - startedAt
  };
}

async function main() {
  const startedAt = Date.now();

  const provider = await benchmarkProviderCase(PROVIDER_CASE);
  const direct = await benchmarkDirectCase(DIRECT_CASE);
  const noResult = await benchmarkNoResultCase(NO_RESULT_CASE);

  const summary = {
    startedAt: new Date(startedAt).toISOString(),
    finishedAt: new Date().toISOString(),
    totalDurationMs: Date.now() - startedAt,
    provider,
    direct,
    noResult
  };

  console.log('=== MKVDrama E2E Benchmark ===');
  console.log(JSON.stringify(summary, null, 2));

  const failed = !provider.playable || !direct.playable;
  process.exit(failed ? 1 : 0);
}

export {
  PROVIDER_CASE,
  DIRECT_CASE,
  NO_RESULT_CASE,
  benchmarkProviderCase,
  benchmarkDirectCase,
  benchmarkNoResultCase,
  main
};

const isDirectRun = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);

if (isDirectRun) {
  main().catch((error) => {
    console.error('MKVDrama benchmark failed:', error);
    process.exit(1);
  });
}
