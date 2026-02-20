import puppeteer from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';
import axios from 'axios';

puppeteer.use(StealthPlugin());

const CHROMIUM_PATH = '/snap/bin/chromium';

async function getProxies() {
    const resp = await axios.get('https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt', { timeout: 10000 });
    const lines = resp.data.trim().split('\n').filter(Boolean);
    // shuffle and take first 20
    return lines.sort(() => Math.random() - 0.5).slice(0, 20).map(l => `socks5://${l.trim()}`);
}

async function tryProxy(proxyUrl) {
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
        timeout: 20000
    });

    try {
        const page = await browser.newPage();
        page.setDefaultNavigationTimeout(15000);

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
        console.log(`  nginx=${isNginx} captcha=${isCaptcha} results=${hasResults} (${resultCount})`);
        if (hasResults) console.log('\n✅ SUCCESS!');
        return { isNginx, isCaptcha, hasResults };
    } finally {
        await browser.close();
    }
}

console.log('Fetching fresh proxies...');
const proxies = await getProxies();
console.log(`Testing ${proxies.length} proxies with stealth browser + --ignore-certificate-errors\n`);

let captchaCount = 0, nginxCount = 0;

for (const proxy of proxies) {
    process.stdout.write(`Proxy: ${proxy} ... `);
    try {
        const r = await tryProxy(proxy);
        if (r.hasResults) process.exit(0);
        if (r.isCaptcha) { captchaCount++; process.stdout.write('CAPTCHA\n'); }
        else if (r.isNginx) { nginxCount++; process.stdout.write('nginx\n'); }
        else process.stdout.write('no results\n');
    } catch (e) {
        const msg = e.message.slice(0, 80);
        if (msg.includes('timeout') || msg.includes('TIMEOUT')) process.stdout.write('timeout\n');
        else process.stdout.write(`failed: ${msg}\n`);
    }
}

console.log(`\n❌ Done. captcha=${captchaCount} nginx=${nginxCount} out of ${proxies.length}`);
