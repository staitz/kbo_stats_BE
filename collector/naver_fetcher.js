// collector/naver_fetcher.js
const url = process.argv[2];
if (!url) {
    console.error("No URL provided");
    process.exit(1);
}

fetch(url, {
    headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*"
    }
})
.then(r => r.json())
.then(data => {
    console.log(JSON.stringify(data));
})
.catch(e => {
    console.error(e.toString());
    process.exit(1);
});
