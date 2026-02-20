import puppeteer from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import axios from 'axios';

puppeteer.use(StealthPlugin());

const CHROMIUM_PATH = '/snap/bin/chromium';

async function getProxies() {
    const resp = await axios.get('https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt', { timeout: 10000 });
    const lines = resp.data.trim().split('\n').filter(Boolean);
    return lines.sort(() => Math.random() - 0.5).slice(0, 8).map(l => `socks5://${l.trim()}`);
}

async function tryProxy(proxyUrl) {
    const browser = await puppeteer.launch({
        executablePath: CHROMIUM_PATH,
        headless: true,
        args: [
            `--proxy-server=${proxyUrl}`,
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--no-first-run',
            '--no-zygote',
        ],
        timeout: 25000
    });

    try {
        const page = await browser.newPage();
        page.setDefaultNavigationTimeout(20000);
        page.setDefaultTimeout(20000);

        await page.goto('https://www.btdig.com/search?q=matrix&order=0', {
            waitUntil: 'domcontentloaded'
        });

        const html = await page.content();
        const title = await page.title();
        const isNginx = html.includes('Welcome to nginx');
        const isCaptcha = html.includes('g-recaptcha') || html.includes('One more step') || html.includes('security check');
        const hasResults = html.includes('one_result');

        console.log(`  title: "${title.slice(0, 60)}"`);
        console.log(`  nginx=${isNginx} captcha=${isCaptcha} results=${hasResults}`);
        return { html, isNginx, isCaptcha, hasResults };
    } finally {
        await browser.close();
    }
}

console.log('Fetching proxy list...');
const proxies = await getProxies();
console.log(`Trying ${proxies.length} proxies with stealth Chromium...\n`);

for (const proxy of proxies) {
    console.log(`Proxy: ${proxy}`);
    try {
        const result = await tryProxy(proxy);
        if (result.hasResults) {
            console.log('\n✅ SUCCESS — stealth browser got BTDigg results!');
            process.exit(0);
        } else if (result.isCaptcha) {
            console.log('  → CAPTCHA\n');
        } else if (result.isNginx) {
            console.log('  → nginx (geo-blocked)\n');
        } else {
            console.log('  → unexpected page\n');
        }
    } catch (e) {
        console.log(`  → failed: ${e.message.slice(0, 100)}\n`);
    }
}
console.log('\n❌ All proxies exhausted — stealth browser cannot bypass BTDigg CAPTCHA');
