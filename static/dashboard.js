// Auto-refresh every 3 seconds
async function refreshData() {
    try {
        const statusResp = await fetch('/api/status');
        const status = await statusResp.json();
        document.getElementById('market-status').textContent = status.market_status;
        document.getElementById('market-status').className = status.market_status.includes('OPEN') ? 'text-2xl mb-6 p-4 rounded-lg font-bold bg-green-500' : 'text-2xl mb-6 p-4 rounded-lg font-bold bg-red-500';
        
        const perfResp = await fetch('/api/performance');
        const perf = await perfResp.json();
        document.getElementById('performance-cards').innerHTML = `
            <div class="bg-gray-800 p-6 rounded-xl">
                <h3 class="text-lg opacity-75">Win Rate</h3>
                <div class="text-3xl font-bold text-green-400">${perf.win_rate}%</div>
            </div>
            <div class="bg-gray-800 p-6 rounded-xl">
                <h3 class="text-lg opacity-75">Daily P&L</h3>
                <div class="text-3xl font-bold ${perf.daily_pnl >= 0 ? 'text-green-400' : 'text-red-400'}">${perf.daily_pnl.toFixed(2)}%</div>
            </div>
            <div class="bg-gray-800 p-6 rounded-xl">
                <h3 class="text-lg opacity-75">Equity</h3>
                <div class="text-3xl font-bold text-blue-400">$${perf.current_equity.toLocaleString()}</div>
            </div>
        `;
        
        const symbolsResp = await fetch('/api/symbols');
        const symbols = await symbolsResp.json();
        // Render symbols grid...
        
        const logsResp = await fetch('/api/logs');
        const logs = await logsResp.json();
        document.getElementById('logs').innerHTML = logs.slice(-20).join('<br>');
        
    } catch(e) {
        console.error('Refresh error:', e);
    }
}

// Start auto-refresh
refreshData();
setInterval(refreshData, 3000);
