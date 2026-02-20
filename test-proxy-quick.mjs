import puppeteer from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';

puppeteer.use(StealthPlugin());

const CHROMIUM_PATH = '/snap/bin/chromium';
const proxyUrl = 'socks5://100.109.163.45:1080';

console.log(`Testing proxy: ${proxyUrl}`);

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
    await page.setViewport({ width: 1280, height: 800 });
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0');

    await page.goto('https://www.btdig.com/search?q=matrix&order=0', { waitUntil: 'domcontentloaded' });

    const html = await page.content();
    const title = await page.title();
    const isNginx = html.includes('Welcome to nginx');
    const isCaptcha = html.includes('g-recaptcha') || html.includes('One more step') || html.includes('security check');
    const hasResults = html.includes('one_result');
    const resultCount = (html.match(/class="one_result"/g) || []).length;

    console.log(`title: "${title.slice(0, 80)}"`);
    console.log(`nginx=${isNginx} captcha=${isCaptcha} results=${hasResults} (${resultCount} found)`);
    if (hasResults) console.log('\n✅ SUCCESS!');
    else if (isCaptcha) console.log('❌ CAPTCHA');
    else if (isNginx) console.log('❌ nginx blocked');
    else console.log('❌ unexpected page');
} catch (e) {
    console.log(`Error: ${e.message}`);
} finally {
    await browser.close();
}
