import puppeteer from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';

puppeteer.use(StealthPlugin());

const CHROMIUM_PATH = '/snap/bin/chromium';
const PROXY = 'socks5://100.109.163.45:1080';
const QUERY = 'matrix';
const PAGES_TO_FETCH = 3;
const CONCURRENT_REQUESTS = 3;

async function fetchPage(browser, pageNum) {
    const start = Date.now();
    const page = await browser.newPage();
    try {
        page.setDefaultNavigationTimeout(30000);
        await page.setViewport({ width: 1280, height: 800 });
        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0');

        const url = `https://www.btdig.com/search?q=${QUERY}&p=${pageNum}&order=0`;
        await page.goto(url, { waitUntil: 'domcontentloaded' });

        const html = await page.content();
        const isNginx = html.includes('Welcome to nginx');
        const isCaptcha = html.includes('g-recaptcha') || html.includes('One more step');
        const hasResults = html.includes('one_result');
        const resultCount = (html.match(/class="one_result"/g) || []).length;
        const elapsed = Date.now() - start;

        return { pageNum, elapsed, isNginx, isCaptcha, hasResults, resultCount };
    } finally {
        await page.close();
    }
}

async function runSequential() {
    console.log('\n=== Sequential (1 browser, N pages) ===');
    const browser = await puppeteer.launch({
        executablePath: CHROMIUM_PATH,
        headless: true,
        args: [
            `--proxy-server=${PROXY}`,
            '--ignore-certificate-errors',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--no-first-run',
            '--disable-blink-features=AutomationControlled',
        ],
        timeout: 30000
    });

    const start = Date.now();
    for (let p = 0; p < PAGES_TO_FETCH; p++) {
        const r = await fetchPage(browser, p);
        console.log(`  page ${r.pageNum}: ${r.resultCount} results, ${r.elapsed}ms, captcha=${r.isCaptcha}`);
    }
    console.log(`  Total: ${Date.now() - start}ms`);
    await browser.close();
}

async function runConcurrent() {
    console.log(`\n=== Concurrent (${CONCURRENT_REQUESTS} browsers at once) ===`);
    const start = Date.now();

    const results = await Promise.allSettled(
        Array.from({ length: CONCURRENT_REQUESTS }, (_, i) => async () => {
            const browser = await puppeteer.launch({
                executablePath: CHROMIUM_PATH,
                headless: true,
                args: [
                    `--proxy-server=${PROXY}`,
                    '--ignore-certificate-errors',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--no-first-run',
                    '--disable-blink-features=AutomationControlled',
                ],
                timeout: 30000
            });
            try {
                return await fetchPage(browser, i);
            } finally {
                await browser.close();
            }
        }).map(fn => fn())
    );

    for (const r of results) {
        if (r.status === 'fulfilled') {
            const { pageNum, resultCount, elapsed, isCaptcha } = r.value;
            console.log(`  page ${pageNum}: ${resultCount} results, ${elapsed}ms, captcha=${isCaptcha}`);
        } else {
            console.log(`  FAILED: ${r.reason?.message?.slice(0, 80)}`);
        }
    }
    console.log(`  Total wall time: ${Date.now() - start}ms`);
}

// Check memory before
const memBefore = process.memoryUsage();
console.log(`Memory before: ${Math.round(memBefore.rss / 1024 / 1024)}MB RSS`);

await runSequential();

const memAfter1 = process.memoryUsage();
console.log(`Memory after sequential: ${Math.round(memAfter1.rss / 1024 / 1024)}MB RSS`);

await runConcurrent();

const memAfter2 = process.memoryUsage();
console.log(`\nMemory after concurrent: ${Math.round(memAfter2.rss / 1024 / 1024)}MB RSS`);
