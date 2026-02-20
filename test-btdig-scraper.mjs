import { searchBtdig } from './lib/scrapers/public-trackers/btdig.js';

console.log('Testing BTDigg scraper with stealth browser...');
console.log('BTDIG_STEALTH_PROXY:', process.env.BTDIG_STEALTH_PROXY);

const start = Date.now();
const results = await searchBtdig('matrix reloaded', null, 'TEST', {
    BTDIG_LIMIT: 20,
    BTDIG_MAX_PAGES: 2,
    BTDIG_STEALTH_PROXY: process.env.BTDIG_STEALTH_PROXY,
    BTDIG_URL: 'https://btdig.com',
    SCRAPER_TIMEOUT: 60000
});
const elapsed = Date.now() - start;
console.log(`\nResults: ${results.length} in ${elapsed}ms`);
if (results.length > 0) {
    results.slice(0, 5).forEach((r, i) => console.log(`  ${i+1}. ${r.Title} (${r.InfoHash?.slice(0,8)})`));
}
