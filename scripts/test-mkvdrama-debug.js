/**
 * Debug mkvdrama for specific title
 */
import { config } from 'dotenv';
config();

const { scrapeMkvDramaSearch, loadMkvDramaContent } = await import('../lib/http-streams/providers/mkvdrama/search.js');
const { resolveHttpStreamUrl } = await import('../lib/http-streams/resolvers/http-resolver.js');

async function main() {
    const query = 'Undercover Miss Hong';
    console.log(`[TEST] Searching for: ${query}`);

    const results = await scrapeMkvDramaSearch(query);
    console.log(`[TEST] Found ${results.length} results`);

    if (results.length > 0) {
        console.log(`[TEST] First result: ${results[0].title} - ${results[0].url}`);

        const content = await loadMkvDramaContent(results[0].url, null, { season: 1, episode: 1 });
        console.log(`[TEST] Title: ${content.title}`);
        console.log(`[TEST] Download links: ${content.downloadLinks?.length || 0}`);

        if (content.downloadLinks?.length > 0) {
            console.log('[TEST] Links:');
            for (const link of content.downloadLinks.slice(0, 5)) {
                console.log(`  - ${link.quality || link.label}: ${link.url}`);
            }

            // Try to resolve the first link
            const firstLink = content.downloadLinks[0];
            console.log(`\n[TEST] Resolving first link: ${firstLink.url}`);
            try {
                const resolved = await resolveHttpStreamUrl(firstLink.url);
                console.log(`[TEST] Resolved to: ${resolved}`);
            } catch (err) {
                console.error(`[TEST] Resolution error: ${err.message}`);
            }
        }
    }

    console.log('\n[TEST] Done');
    process.exit(0);
}

main().catch(err => {
    console.error('[TEST] Fatal error:', err.message);
    process.exit(1);
});
