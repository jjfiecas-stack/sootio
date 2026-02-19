/**
 * Shared Cloudflare Cookie Manager
 * Provides cookie caching for FlareSolverr-solved Cloudflare challenges.
 * Used by scrapers that are Cloudflare-blocked (TPB, MagnetDL, TorrentDownload, etc.)
 *
 * Pattern: in-memory Map + SQLite persistence (same as ExtTo's proven approach)
 */

import axios from 'axios';
import * as SqliteCache from '../../util/cache-store.js';

const COOKIE_CACHE_SERVICE = 'cf_cookie';
const cookieMemCache = new Map(); // domain -> { cookieHeader, userAgent, timestamp }

/**
 * Get cached Cloudflare cookie for a domain.
 * Checks in-memory cache first, then SQLite.
 * @param {string} domain - e.g. 'apibay.org'
 * @returns {Promise<{cookieHeader: string, userAgent: string}|null>}
 */
export async function getCachedCfCookie(domain) {
    try {
        const memCached = cookieMemCache.get(domain);
        if (memCached?.cookieHeader && memCached?.userAgent) {
            return memCached;
        }

        const cacheKey = `${domain}_cf_cookie`;
        const result = await SqliteCache.getCachedRecord(COOKIE_CACHE_SERVICE, cacheKey);
        if (result?.data && (result.data.cookieHeader || result.data.cfClearance) && result.data.userAgent) {
            const cookieHeader = result.data.cookieHeader || `cf_clearance=${result.data.cfClearance}`;
            const hydrated = {
                cookieHeader,
                userAgent: result.data.userAgent,
                timestamp: result.data.timestamp
            };
            cookieMemCache.set(domain, hydrated);
            return hydrated;
        }
    } catch {
        // Ignore cache errors
    }
    return null;
}

/**
 * Save Cloudflare cookie to both in-memory and SQLite cache.
 * @param {string} domain - Domain the cookie is for
 * @param {Array} cookies - FlareSolverr cookie array [{name, value}, ...]
 * @param {string} userAgent - User-Agent that was used (must match for cookie to work)
 * @returns {Promise<{cookieHeader: string, userAgent: string}|null>}
 */
export async function saveCfCookie(domain, cookies, userAgent) {
    try {
        const cookieHeader = extractCookieHeader(cookies);
        const cfClearance = extractCfClearance(cookies);

        if (!cookieHeader && !cfClearance) return null;

        const finalCookieHeader = cookieHeader || `cf_clearance=${cfClearance}`;
        const cacheKey = `${domain}_cf_cookie`;
        const cookieData = {
            cfClearance,
            cookieHeader: finalCookieHeader,
            userAgent,
            timestamp: Date.now()
        };

        cookieMemCache.set(domain, cookieData);
        await SqliteCache.upsertCachedMagnet({
            service: COOKIE_CACHE_SERVICE,
            hash: cacheKey,
            data: cookieData
        });

        return { cookieHeader: finalCookieHeader, userAgent };
    } catch {
        // Ignore cache errors
        return null;
    }
}

/**
 * Clear cached cookie for a domain (when it stops working).
 * @param {string} domain
 */
export async function clearCfCookie(domain) {
    if (!domain) return;
    cookieMemCache.delete(domain);
    try {
        await SqliteCache.deleteCachedHash(COOKIE_CACHE_SERVICE, `${domain}_cf_cookie`);
    } catch {
        // Ignore cache errors
    }
}

/**
 * Full flow: call FlareSolverr, extract cookies, cache them.
 * @param {string} domain - Domain being solved
 * @param {string} url - URL to fetch via FlareSolverr
 * @param {string} flareSolverrUrl - FlareSolverr service URL
 * @param {number} timeout - Request timeout in ms
 * @param {string} logPrefix - Logging prefix
 * @param {string} scraperName - Scraper name for logging
 * @param {object} [options] - Optional settings
 * @param {object} [options.proxy] - Proxy for FlareSolverr's Chromium, e.g. { url: 'socks5://ip:port' }
 * @returns {Promise<{body: string, cookieHeader: string, userAgent: string}|null>}
 */
export async function solveAndCache(domain, url, flareSolverrUrl, timeout, logPrefix, scraperName, options = {}) {
    if (!flareSolverrUrl) return null;

    const flareTimeout = Math.max(timeout * 3, 30000);

    try {
        const proxyLabel = options.proxy?.url ? ` via proxy ${options.proxy.url}` : '';
        console.log(`[${logPrefix} SCRAPER] ${scraperName} calling FlareSolverr for ${url}${proxyLabel}`);
        const requestBody = {
            cmd: 'request.get',
            url,
            maxTimeout: flareTimeout
        };
        if (options.proxy?.url) {
            requestBody.proxy = options.proxy;
        }
        const response = await axios.post(`${flareSolverrUrl}/v1`, requestBody, {
            timeout: flareTimeout + 5000,
            headers: { 'Content-Type': 'application/json' }
        });

        const solution = response?.data?.solution;
        if (!solution?.response) {
            console.log(`[${logPrefix} SCRAPER] ${scraperName} FlareSolverr returned no response`);
            return null;
        }

        const body = solution.response;

        // Check if FlareSolverr still got a Cloudflare challenge
        if (isCloudflareChallenge(body)) {
            console.log(`[${logPrefix} SCRAPER] ${scraperName} FlareSolverr still blocked by Cloudflare`);
            return null;
        }

        // Extract and cache cookies
        const cookies = solution.cookies;
        const userAgent = solution.userAgent || 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36';
        let cookieHeader = null;

        if (cookies && Array.isArray(cookies) && cookies.length > 0) {
            const saved = await saveCfCookie(domain, cookies, userAgent);
            if (saved) {
                cookieHeader = saved.cookieHeader;
                console.log(`[${logPrefix} SCRAPER] ${scraperName} cached CF cookie for ${domain}`);
            }
        }

        console.log(`[${logPrefix} SCRAPER] ${scraperName} FlareSolverr success (status: ${solution.status || 'n/a'})`);
        return { body, cookieHeader, userAgent };
    } catch (error) {
        console.log(`[${logPrefix} SCRAPER] ${scraperName} FlareSolverr error: ${error.message}`);
        return null;
    }
}

/**
 * Check if response text is a Cloudflare challenge page.
 * @param {string} text
 * @returns {boolean}
 */
export function isCloudflareChallenge(text = '') {
    if (!text) return false;
    return text.includes('Just a moment') ||
        text.includes('cf-browser-verification') ||
        text.includes('Enable JavaScript and cookies') ||
        text.includes('Attention Required');
}

// --- Internal helpers ---

function extractCfClearance(cookies) {
    if (!Array.isArray(cookies)) return null;
    const cfCookie = cookies.find(c => c.name === 'cf_clearance');
    return cfCookie?.value || null;
}

function extractCookieHeader(cookies) {
    if (!Array.isArray(cookies) || cookies.length === 0) return null;
    return cookies
        .map(c => `${c.name}=${c.value}`)
        .join('; ');
}
