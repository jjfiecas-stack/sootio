import puppeteer from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';

puppeteer.use(StealthPlugin());

const CHROMIUM_PATH = '/snap/bin/chromium';

// Use proxies that we know can reach btdig.com (from previous test runs)
const PROXIES = [
    'socks5://174.77.111.197:4145',  // reached btdig.com via FlareSolverr previously
    'socks5://72.195.34.60:27391',   // bypassed TPB CF successfully
    'socks5://72.195.114.184:4145',  // another known-good from TPB test
    'socks5://156.238.242.13:10080', // reached btdig.com via SOCKS rotation previously
];

async function tryProxy(proxyUrl) {
    console.log(`\nProxy: ${proxyUrl}`);
    const browser = await puppeteer.launch({
        executablePath: CHROMIUM_PATH,
        headless: true,
        args: [
            `--proxy-server=${proxyUrl}`,
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
        const page = await browser.newPage();
        page.setDefaultNavigationTimeout(25000);

        // Set realistic viewport and user agent
        await page.setViewport({ width: 1280, height: 800 });
        await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0');

        await page.goto('https://www.btdig.com/search?q=matrix&order=0', {
            waitUntil: 'domcontentloaded'
        });

        const html = await page.content();
        const title = await page.title();
        const isNginx = html.includes('Welcome to nginx');
        const isCaptcha = html.includes('g-recaptcha') || html.includes('One more step') || html.includes('security check');
        const hasResults = html.includes('one_result');
        const resultCount = (html.match(/class="one_result"/g) || []).length;

        console.log(`  title: "${title.slice(0, 70)}"`);
        console.log(`  nginx=${isNginx} captcha=${isCaptcha} results=${hasResults} (${resultCount} found)`);
        if (hasResults) console.log('\n✅ SUCCESS!');
        return { isNginx, isCaptcha, hasResults };
    } finally {
        await browser.close();
    }
}

for (const proxy of PROXIES) {
    try {
        const r = await tryProxy(proxy);
        if (r.hasResults) process.exit(0);
    } catch (e) {
        console.log(`  → failed: ${e.message.slice(0, 100)}`);
    }
}
console.log('\n❌ All known-good proxies failed with stealth browser');
