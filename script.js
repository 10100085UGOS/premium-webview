// Coin data with icons
const coins = [
    { id: 'bitcoin', symbol: 'BTC', name: 'BTC', icon: '₿', iconClass: 'icon-btc' },
    { id: 'ethereum', symbol: 'ETH', name: 'ETH', icon: 'Ξ', iconClass: 'icon-eth' },
    { id: 'binancecoin', symbol: 'BNB', name: 'BNB', icon: 'BNB', iconClass: 'icon-bnb' },
    { id: 'ripple', symbol: 'XRP', name: 'XRP', icon: 'XRP', iconClass: 'icon-xrp' },
    { id: 'dogecoin', symbol: 'DOGE', name: 'DOGE', icon: 'Ð', iconClass: 'icon-doge' },
    { id: 'tether', symbol: 'USDT', name: 'USDT', icon: '₮', iconClass: 'icon-usdt' },
    { id: 'usd-coin', symbol: 'USDC', name: 'USDC', icon: '₵', iconClass: 'icon-usdc' }
];

// Format market cap
function formatMarketCap(cap) {
    if (cap >= 1e12) return `$${(cap/1e12).toFixed(2)}T`;
    if (cap >= 1e9) return `$${(cap/1e9).toFixed(2)}B`;
    if (cap >= 1e6) return `$${(cap/1e6).toFixed(2)}M`;
    return `$${cap.toFixed(0)}`;
}

// Format price
function formatPrice(price) {
    if (price < 1) return `$${price.toFixed(4)}`;
    if (price < 1000) return `$${price.toFixed(2)}`;
    return `$${price.toFixed(0)}`;
}

// Fetch data from CoinCap
async function fetchMarketData() {
    try {
        const response = await fetch('https://api.coincap.io/v2/assets?ids=' + coins.map(c => c.id).join(','));
        const data = await response.json();
        return data.data;
    } catch (error) {
        console.error('Error fetching data:', error);
        return null;
    }
}

// Update UI
function updateUI(marketData) {
    const coinList = document.getElementById('coin-list');
    coinList.innerHTML = '';

    marketData.forEach((coin, index) => {
        const coinInfo = coins.find(c => c.id === coin.id);
        if (!coinInfo) return;

        const change = parseFloat(coin.changePercent24Hr);
        const changeClass = change >= 0 ? 'change-positive' : 'change-negative';
        const changeArrow = change >= 0 ? '▲' : '▼';

        const coinItem = document.createElement('div');
        coinItem.className = 'coin-item';
        coinItem.innerHTML = `
            <div class="coin-left">
                <span class="coin-icon ${coinInfo.iconClass}">${coinInfo.icon}</span>
                <span class="coin-name">${coinInfo.name}</span>
            </div>
            <span class="coin-marketcap">${formatMarketCap(parseFloat(coin.marketCapUsd))}</span>
            <span class="coin-price">${formatPrice(parseFloat(coin.priceUsd))}</span>
            <span class="coin-change ${changeClass}">${changeArrow} ${Math.abs(change).toFixed(2)}%</span>
        `;
        coinList.appendChild(coinItem);
    });

    // Update timestamp
    document.getElementById('timestamp').innerText = new Date().toLocaleTimeString('en-IN', { hour12: false });
}

// Initialize and update every 10 seconds
async function init() {
    const data = await fetchMarketData();
    if (data) updateUI(data);

    // Update every 10 seconds
    setInterval(async () => {
        const newData = await fetchMarketData();
        if (newData) updateUI(newData);
    }, 10000);
}

// Bitcoin Chart
async function loadBitcoinChart() {
    try {
        // Fetch 7 days of Bitcoin price from Binance
        const response = await fetch('https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1d&limit=7');
        const data = await response.json();
        
        const dates = data.map(k => {
            const date = new Date(k[0]);
            return `${date.getDate()}/${date.getMonth()+1}`;
        });
        const prices = data.map(k => parseFloat(k[4]));

        const ctx = document.getElementById('btcChart').getContext('2d');
        new Chart(ctx, {
            type: 'line',
            data: {
                labels: dates,
                datasets: [{
                    label: 'BTC Price (USD)',
                    data: prices,
                    borderColor: '#3b82f6',
                    backgroundColor: 'rgba(59,130,246,0.1)',
                    borderWidth: 3,
                    pointBackgroundColor: '#3b82f6',
                    pointBorderColor: 'white',
                    pointRadius: 4,
                    tension: 0.2,
                    fill: true
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: { display: false },
                    tooltip: { mode: 'index', intersect: false }
                },
                scales: {
                    y: { grid: { color: '#334155' }, ticks: { color: '#94a3b8' } },
                    x: { grid: { display: false }, ticks: { color: '#94a3b8' } }
                }
            }
        });
    } catch (error) {
        console.error('Chart error:', error);
    }
}

// Start everything
window.onload = () => {
    init();
    loadBitcoinChart();
};
