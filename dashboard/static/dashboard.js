// ═════════════════════════════════════════════════════════════════
// HargaPangan Dashboard - JavaScript Logic
// Chart rendering, data fetching, auto-refresh
// ═════════════════════════════════════════════════════════════════

let volatilitasChartInstance = null;
let trenChartInstance = null;

// ─────────────────────────────────────────────
// UTILITY FUNCTIONS
// ─────────────────────────────────────────────

function hideLoadingOverlay() {
    const overlay = document.getElementById('loadingOverlay');
    if (overlay) {
        overlay.classList.add('hidden');
    }
}

function formatCurrency(value) {
    if (!value) return 'Rp 0';
    return new Intl.NumberFormat('id-ID', {
        style: 'currency',
        currency: 'IDR',
        minimumFractionDigits: 0,
        maximumFractionDigits: 0
    }).format(value).replace('Rp', 'Rp ');
}

function formatDate(dateString) {
    if (!dateString) return '-';
    try {
        return new Date(dateString).toLocaleString('id-ID');
    } catch {
        return dateString;
    }
}

function getVolatilityBadge(volatilitas) {
    if (volatilitas > 20) return { class: 'badge-waspada', text: 'CRITICAL' };
    if (volatilitas > 10) return { class: 'badge-waspada', text: 'WARNING' };
    return { class: 'badge-stabil', text: 'NORMAL' };
}

// ─────────────────────────────────────────────
// FETCH DATA
// ─────────────────────────────────────────────

async function fetchAnalysisData() {
    try {
        const response = await fetch('/api/data', { cache: 'no-cache' });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const data = await response.json();
        return data;
    } catch (error) {
        console.error('Error fetching analysis data:', error);
        showErrorMessage('Gagal memuat data analisis dari Spark');
        return null;
    }
}

async function fetchLiveData() {
    try {
        const response = await fetch('/api/live', { cache: 'no-cache' });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const data = await response.json();
        return data;
    } catch (error) {
        console.error('Error fetching live data:', error);
        return null;
    }
}

// ─────────────────────────────────────────────
// RENDER CHARTS
// ─────────────────────────────────────────────

function renderVolatilitasChart(volatilitasData) {
    const ctx = document.getElementById('volatilitasChart');
    if (!ctx) return;

    const labels = volatilitasData.map(d => d.label || d.komoditas);
    const values = volatilitasData.map(d => parseFloat(d.indeks_volatilitas) || 0);
    
    // Color based on volatility level
    const colors = values.map(v => {
        if (v > 20) return 'rgb(239, 68, 68)';     // Red - Critical
        if (v > 10) return 'rgb(245, 158, 11)';    // Orange - Warning
        return 'rgb(16, 185, 129)';                // Green - Normal
    });

    if (volatilitasChartInstance) {
        volatilitasChartInstance.destroy();
    }

    volatilitasChartInstance = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Indeks Volatilitas (%)',
                data: values,
                backgroundColor: colors,
                borderColor: colors,
                borderWidth: 1,
                borderRadius: 6
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true,
                    labels: {
                        color: '#f1f5f9',
                        font: { size: 12 }
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(15, 23, 42, 0.8)',
                    titleColor: '#f1f5f9',
                    bodyColor: '#cbd5e1',
                    callbacks: {
                        label: function(context) {
                            return `Volatilitas: ${context.parsed.y.toFixed(2)}%`;
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        color: '#94a3b8',
                        callback: function(value) {
                            return value.toFixed(0) + '%';
                        }
                    },
                    grid: { color: 'rgba(148, 163, 184, 0.1)' }
                },
                x: {
                    ticks: { color: '#94a3b8' },
                    grid: { display: false }
                }
            }
        }
    });
}

function renderTrenChart(trenData) {
    const ctx = document.getElementById('trenChart');
    if (!ctx) return;

    const labels = trenData.map(d => d.label || d.komoditas);
    const avgPrices = trenData.map(d => parseFloat(d.harga_rata2) || 0);
    const minPrices = trenData.map(d => parseFloat(d.harga_min_sepanjang_waktu) || 0);
    const maxPrices = trenData.map(d => parseFloat(d.harga_max_sepanjang_waktu) || 0);

    if (trenChartInstance) {
        trenChartInstance.destroy();
    }

    trenChartInstance = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Harga Rata-rata',
                    data: avgPrices,
                    borderColor: 'rgb(16, 185, 129)',
                    backgroundColor: 'rgba(16, 185, 129, 0.1)',
                    fill: true,
                    tension: 0.4,
                    borderWidth: 2,
                    pointRadius: 5,
                    pointBackgroundColor: 'rgb(16, 185, 129)',
                    pointBorderColor: 'rgba(16, 185, 129, 0.3)',
                    pointBorderWidth: 2
                },
                {
                    label: 'Harga Min',
                    data: minPrices,
                    borderColor: 'rgba(59, 130, 246, 0.5)',
                    borderDash: [5, 5],
                    fill: false,
                    tension: 0.4,
                    borderWidth: 1,
                    pointRadius: 0
                },
                {
                    label: 'Harga Max',
                    data: maxPrices,
                    borderColor: 'rgba(239, 68, 68, 0.5)',
                    borderDash: [5, 5],
                    fill: false,
                    tension: 0.4,
                    borderWidth: 1,
                    pointRadius: 0
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: {
                    display: true,
                    labels: {
                        color: '#f1f5f9',
                        font: { size: 12 }
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(15, 23, 42, 0.8)',
                    titleColor: '#f1f5f9',
                    bodyColor: '#cbd5e1',
                    callbacks: {
                        label: function(context) {
                            return `${context.dataset.label}: ${formatCurrency(context.parsed.y)}`;
                        }
                    }
                }
            },
            scales: {
                y: {
                    ticks: {
                        color: '#94a3b8',
                        callback: function(value) {
                            return formatCurrency(value);
                        }
                    },
                    grid: { color: 'rgba(148, 163, 184, 0.1)' }
                },
                x: {
                    ticks: { color: '#94a3b8' },
                    grid: { display: false }
                }
            }
        }
    });
}

// ─────────────────────────────────────────────
// RENDER TABLES
// ─────────────────────────────────────────────

function renderVolatilitasTable(volatilitasData) {
    const tbody = document.getElementById('volatilitasTable');
    if (!tbody) return;

    if (!volatilitasData || volatilitasData.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" style="text-align: center;">Tidak ada data</td></tr>';
        return;
    }

    tbody.innerHTML = volatilitasData.map((item, idx) => {
        const vol = parseFloat(item.indeks_volatilitas) || 0;
        const badge = getVolatilityBadge(vol);
        
        return `
            <tr>
                <td>${item.komoditas || '-'}</td>
                <td>${item.label || '-'}</td>
                <td>${formatCurrency(parseFloat(item.harga_max) || 0)}</td>
                <td>${formatCurrency(parseFloat(item.harga_min) || 0)}</td>
                <td>${formatCurrency(parseFloat(item.harga_avg) || 0)}</td>
                <td>${(parseFloat(item.harga_stddev) || 0).toFixed(2)}</td>
                <td><span class="badge ${badge.class}">${badge.text} (${vol.toFixed(2)}%)</span></td>
            </tr>
        `;
    }).join('');

    document.getElementById('volatilitasUpdate').textContent = 'Diperbarui: ' + formatDate(new Date());
}

function renderKorelasiTable(korelasiData) {
    const tbody = document.getElementById('korelasiTable');
    if (!tbody) return;

    if (!korelasiData || korelasiData.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" style="text-align: center;">Tidak ada data</td></tr>';
        return;
    }

    tbody.innerHTML = korelasiData.map((item, idx) => {
        const vol = parseFloat(item.indeks_volatilitas) || 0;
        const mentions = parseInt(item.jumlah_sebutan) || 0;
        const status = item.status_korelasi || 'RENDAH';
        
        let statusBadgeClass = 'badge-rendah';
        if (status.includes('TINGGI')) statusBadgeClass = 'badge-waspada';
        else if (status.includes('MODERAT')) statusBadgeClass = 'badge-waspada';
        
        return `
            <tr>
                <td>${item.komoditas || '-'}</td>
                <td>${item.label || '-'}</td>
                <td>${vol.toFixed(2)}%</td>
                <td>${formatCurrency(parseFloat(item.harga_avg) || 0)}</td>
                <td>${mentions}</td>
                <td><span class="badge ${statusBadgeClass}">${status.split(':')[0]}</span></td>
            </tr>
        `;
    }).join('');

    document.getElementById('korelasiUpdate').textContent = 'Diperbarui: ' + formatDate(new Date());
}

// ─────────────────────────────────────────────
// RENDER LIVE FEEDS
// ─────────────────────────────────────────────

function renderLiveApiFeed(liveData) {
    const container = document.getElementById('liveApiFeed');
    if (!container) return;

    if (!liveData || !liveData.data || liveData.data.length === 0) {
        container.innerHTML = '<p style="color: var(--text-secondary); text-align: center; padding: 2rem;">Tidak ada data live</p>';
        return;
    }

    const items = liveData.data.slice(-10); // Last 10 items
    container.innerHTML = items.reverse().map(item => {
        const change = parseFloat(item.perubahan_pct) || 0;
        const changeClass = change > 0 ? 'price-up' : change < 0 ? 'price-down' : 'price-neutral';
        const changeSymbol = change > 0 ? '↑' : change < 0 ? '↓' : '→';
        
        return `
            <div class="feed-item">
                <div class="feed-time">${item.timestamp || 'unknown'}</div>
                <div class="feed-title">${item.label || item.komoditas || 'Unknown'}</div>
                <div class="feed-meta">
                    Harga: <strong>${formatCurrency(parseFloat(item.harga) || 0)}</strong>
                    <span class="${changeClass}"> ${changeSymbol} ${Math.abs(change).toFixed(2)}%</span>
                </div>
            </div>
        `;
    }).join('');

    document.getElementById('liveApiUpdate').textContent = 'Diperbarui: ' + formatDate(new Date());
}

function renderLiveRssFeed(liveData) {
    const container = document.getElementById('liveRssFeed');
    if (!container) return;

    if (!liveData || !liveData.data || liveData.data.length === 0) {
        container.innerHTML = '<p style="color: var(--text-secondary); text-align: center; padding: 2rem;">Tidak ada berita</p>';
        return;
    }

    const items = liveData.data.slice(-5); // Last 5 items
    container.innerHTML = items.reverse().map(item => {
        return `
            <div class="feed-item">
                <div class="feed-time">${item.published || item.timestamp || 'unknown'}</div>
                <div class="feed-title">
                    <a href="${item.link || '#'}" target="_blank" style="color: var(--accent-green-light); text-decoration: none;">
                        ${item.title || 'No title'}
                    </a>
                </div>
                <div class="feed-meta">
                    Komoditas: <strong>${item.komoditas || 'umum'}</strong>
                </div>
            </div>
        `;
    }).join('');

    document.getElementById('liveRssUpdate').textContent = 'Diperbarui: ' + formatDate(new Date());
}

// ─────────────────────────────────────────────
// UPDATE SUMMARY CARDS
// ─────────────────────────────────────────────

function updateSummaryCards(analysisData, liveData) {
    // Total data
    const totalApi = analysisData?.metadata?.total_data_api || 0;
    const totalRss = analysisData?.metadata?.total_data_rss || 0;
    const totalKomoditas = analysisData?.analisis_volatilitas?.length || 0;

    document.getElementById('totalApi').textContent = totalApi;
    document.getElementById('totalRss').textContent = totalRss;
    document.getElementById('totalKomoditas').textContent = totalKomoditas;
}

// ─────────────────────────────────────────────
// ERROR HANDLING
// ─────────────────────────────────────────────

function showErrorMessage(message) {
    const container = document.querySelector('.container');
    if (container) {
        const errorDiv = document.createElement('div');
        errorDiv.style.cssText = `
            background: rgba(239, 68, 68, 0.1);
            border: 1px solid rgba(239, 68, 68, 0.3);
            color: #ef4444;
            padding: 1rem 1.5rem;
            border-radius: 0.75rem;
            margin-bottom: 2rem;
            font-size: 0.9rem;
        `;
        errorDiv.textContent = '⚠️ ' + message;
        container.insertBefore(errorDiv, container.firstChild);
    }
}

// ─────────────────────────────────────────────
// MAIN UPDATE FUNCTION
// ─────────────────────────────────────────────

async function updateDashboard() {
    console.log('Fetching data...');
    
    const analysisData = await fetchAnalysisData();
    const liveData = await fetchLiveData();

    if (analysisData) {
        // Update charts
        if (analysisData.analisis_volatilitas) {
            renderVolatilitasChart(analysisData.analisis_volatilitas);
            renderVolatilitasTable(analysisData.analisis_volatilitas);
        }

        if (analysisData.analisis_tren_harga) {
            renderTrenChart(analysisData.analisis_tren_harga);
        }

        if (analysisData.analisis_berita) {
            renderKorelasiTable(analysisData.analisis_berita);
        }

        // Update summary
        updateSummaryCards(analysisData, liveData);
    }

    if (liveData) {
        if (liveData.live_api) {
            renderLiveApiFeed(liveData.live_api);
        }
        if (liveData.live_rss) {
            renderLiveRssFeed(liveData.live_rss);
        }
    }

    hideLoadingOverlay();
    document.getElementById('lastUpdated').textContent = formatDate(new Date());
}

// ─────────────────────────────────────────────
// INITIALIZATION & AUTO-REFRESH
// ─────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', function() {
    console.log('Dashboard initialized');
    
    // Initial load
    updateDashboard();

    // Auto-refresh every 30 seconds
    setInterval(updateDashboard, 30000);
    
    console.log('Auto-refresh enabled (30s interval)');
});
