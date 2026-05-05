let volatilitasChartInstance = null;
let trenChartInstance = null;

function formatRupiah(angka) {
    if (angka === null || angka === undefined || isNaN(angka)) return '-';
    return 'Rp ' + angka.toLocaleString('id-ID');
}

function formatNumber(angka) {
    if (angka === null || angka === undefined || isNaN(angka)) return '-';
    return angka.toLocaleString('id-ID', { maximumFractionDigits: 2 });
}

function getStatusBadgeClass(status) {
    if (!status) return 'badge-stabil';
    if (status.includes('WASPADA')) return 'badge-waspada';
    if (status.includes('RENDAH')) return 'badge-rendah';
    return 'badge-stabil';
}

async function fetchData() {
    try {
        const response = await fetch('/api/data');
        if (!response.ok) throw new Error('Gagal mengambil data');
        return await response.json();
    } catch (error) {
        console.error('Error fetching data:', error);
        return null;
    }
}

async function fetchLiveData() {
    try {
        const response = await fetch('/api/live');
        if (!response.ok) throw new Error('Gagal mengambil data live');
        return await response.json();
    } catch (error) {
        console.error('Error fetching live data:', error);
        return null;
    }
}

function updateSummary(data) {
    const meta = data.metadata || {};
    document.getElementById('totalApi').textContent = meta.total_data_api || 0;
    document.getElementById('totalRss').textContent = meta.total_data_rss || 0;

    const volatilitas = data.analisis_1_volatilitas || [];
    document.getElementById('totalKomoditas').textContent = volatilitas.length;

    const timestamp = data.server_timestamp || new Date().toLocaleString('id-ID');
    document.getElementById('lastUpdated').textContent = 'Update: ' + timestamp;
}

function renderVolatilitasChart(data) {
    const ctx = document.getElementById('volatilitasChart').getContext('2d');
    const volatilitas = data.analisis_1_volatilitas || [];

    if (volatilitasChartInstance) {
        volatilitasChartInstance.destroy();
    }

    const labels = volatilitas.map(v => v.label || v.komoditas);
    const values = volatilitas.map(v => v.indeks_volatilitas || 0);

    volatilitasChartInstance = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'Indeks Volatilitas',
                data: values,
                backgroundColor: values.map(v => v > 10 ? 'rgba(245, 158, 11, 0.8)' : 'rgba(16, 185, 129, 0.8)'),
                borderColor: values.map(v => v > 10 ? 'rgba(245, 158, 11, 1)' : 'rgba(16, 185, 129, 1)'),
                borderWidth: 2,
                borderRadius: 8,
                borderSkipped: false,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: 'rgba(15, 23, 42, 0.9)',
                    titleColor: '#34d399',
                    bodyColor: '#f1f5f9',
                    borderColor: 'rgba(16, 185, 129, 0.3)',
                    borderWidth: 1,
                    padding: 12,
                    callbacks: {
                        label: function(context) {
                            return 'Indeks: ' + formatNumber(context.parsed.y);
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: { color: 'rgba(148, 163, 184, 0.1)' },
                    ticks: { color: '#94a3b8' }
                },
                x: {
                    grid: { display: false },
                    ticks: { color: '#94a3b8' }
                }
            },
            animation: {
                duration: 1000,
                easing: 'easeOutQuart'
            }
        }
    });
}

function renderTrenChart(data) {
    const ctx = document.getElementById('trenChart').getContext('2d');
    const tren = data.analisis_2_tren_harga || [];

    if (trenChartInstance) {
        trenChartInstance.destroy();
    }

    const labels = tren.map(t => t.label || t.komoditas);
    const minValues = tren.map(t => t.harga_terendah_sepanjang || 0);
    const avgValues = tren.map(t => t.harga_rata2_keseluruhan || 0);
    const maxValues = tren.map(t => t.harga_tertinggi_sepanjang || 0);

    trenChartInstance = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Harga Terendah',
                    data: minValues,
                    borderColor: 'rgba(59, 130, 246, 0.8)',
                    backgroundColor: 'rgba(59, 130, 246, 0.1)',
                    tension: 0.4,
                    fill: false,
                    pointRadius: 5,
                    pointHoverRadius: 7,
                    pointBackgroundColor: 'rgba(59, 130, 246, 1)',
                },
                {
                    label: 'Rata-rata',
                    data: avgValues,
                    borderColor: 'rgba(16, 185, 129, 0.8)',
                    backgroundColor: 'rgba(16, 185, 129, 0.1)',
                    tension: 0.4,
                    fill: true,
                    pointRadius: 5,
                    pointHoverRadius: 7,
                    pointBackgroundColor: 'rgba(16, 185, 129, 1)',
                },
                {
                    label: 'Harga Tertinggi',
                    data: maxValues,
                    borderColor: 'rgba(245, 158, 11, 0.8)',
                    backgroundColor: 'rgba(245, 158, 11, 0.1)',
                    tension: 0.4,
                    fill: false,
                    pointRadius: 5,
                    pointHoverRadius: 7,
                    pointBackgroundColor: 'rgba(245, 158, 11, 1)',
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top',
                    labels: { color: '#94a3b8', padding: 20 }
                },
                tooltip: {
                    backgroundColor: 'rgba(15, 23, 42, 0.9)',
                    titleColor: '#34d399',
                    bodyColor: '#f1f5f9',
                    borderColor: 'rgba(16, 185, 129, 0.3)',
                    borderWidth: 1,
                    padding: 12,
                    callbacks: {
                        label: function(context) {
                            return context.dataset.label + ': ' + formatRupiah(context.parsed.y);
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: { color: 'rgba(148, 163, 184, 0.1)' },
                    ticks: {
                        color: '#94a3b8',
                        callback: function(value) {
                            return 'Rp ' + (value / 1000) + 'K';
                        }
                    }
                },
                x: {
                    grid: { display: false },
                    ticks: { color: '#94a3b8' }
                }
            },
            animation: {
                duration: 1000,
                easing: 'easeOutQuart'
            }
        }
    });
}

function renderVolatilitasTable(data) {
    const tbody = document.getElementById('volatilitasTable');
    const volatilitas = data.analisis_1_volatilitas || [];

    if (volatilitas.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" style="text-align: center; color: var(--text-secondary);">Tidak ada data volatilitas</td></tr>';
        return;
    }

    tbody.innerHTML = volatilitas.map(v => `
        <tr>
            <td><strong>${v.komoditas || '-'}</strong></td>
            <td>${v.label || '-'}</td>
            <td>${formatRupiah(v.harga_max)}</td>
            <td>${formatRupiah(v.harga_min)}</td>
            <td>${formatRupiah(v.harga_avg)}</td>
            <td>${formatNumber(v.harga_stddev)}</td>
            <td>
                <span class="badge ${v.indeks_volatilitas > 10 ? 'badge-waspada' : 'badge-stabil'}">
                    ${formatNumber(v.indeks_volatilitas)}
                </span>
            </td>
        </tr>
    `).join('');

    document.getElementById('volatilitasUpdate').textContent = 'Diperbarui: ' + new Date().toLocaleTimeString('id-ID');
}

function renderKorelasiTable(data) {
    const tbody = document.getElementById('korelasiTable');
    const korelasi = data.analisis_3_korelasi_berita || [];

    if (korelasi.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" style="text-align: center; color: var(--text-secondary);">Tidak ada data korelasi</td></tr>';
        return;
    }

    tbody.innerHTML = korelasi.map(k => `
        <tr>
            <td><strong>${k.komoditas || '-'}</strong></td>
            <td>${k.label || '-'}</td>
            <td>${formatNumber(k.indeks_volatilitas)}</td>
            <td>${formatRupiah(k.harga_avg)}</td>
            <td>${k.jumlah_sebutan || 0}</td>
            <td>
                <span class="badge ${getStatusBadgeClass(k.status_korelasi)}">
                    ${k.status_korelasi || '-'}
                </span>
            </td>
        </tr>
    `).join('');

    document.getElementById('korelasiUpdate').textContent = 'Diperbarui: ' + new Date().toLocaleTimeString('id-ID');
}

function renderLiveApi(data) {
    const container = document.getElementById('liveApiFeed');
    const apiData = data.live_api || {};
    const items = apiData.data || [];

    if (items.length === 0) {
        container.innerHTML = '<p style="color: var(--text-secondary); text-align: center; padding: 2rem;">Belum ada data live API</p>';
        return;
    }

    container.innerHTML = items.slice().reverse().map(item => {
        const pct = item.perubahan_pct;
        const indikator = pct > 0 ? '↑' : pct < 0 ? '↓' : '→';
        const priceClass = pct > 0 ? 'price-up' : pct < 0 ? 'price-down' : 'price-neutral';
        const pctStr = pct !== undefined ? `(${indikator}${Math.abs(pct).toFixed(1)}%)` : '';

        return `
            <div class="feed-item">
                <div class="feed-time">${item.jam || item.timestamp_iso || ''}</div>
                <div class="feed-title">${item.label || item.komoditas || 'Unknown'}</div>
                <div class="feed-meta">
                    <span class="${priceClass}">${formatRupiah(item.harga)} / ${item.satuan || 'kg'}</span>
                    <span style="margin-left: 0.5rem; color: var(--text-secondary);">${pctStr}</span>
                    <span style="margin-left: 0.5rem; font-size: 0.75rem; color: var(--text-secondary);">[${item.sumber || 'unknown'}]</span>
                </div>
            </div>
        `;
    }).join('');

    document.getElementById('liveApiUpdate').textContent = 'Diperbarui: ' + new Date().toLocaleTimeString('id-ID');
}

function renderLiveRss(data) {
    const container = document.getElementById('liveRssFeed');
    const rssData = data.live_rss || {};
    const items = rssData.data || [];

    if (items.length === 0) {
        container.innerHTML = '<p style="color: var(--text-secondary); text-align: center; padding: 2rem;">Belum ada berita RSS</p>';
        return;
    }

    container.innerHTML = items.slice().reverse().map(item => {
        return `
            <div class="feed-item">
                <div class="feed-time">${item.timestamp || ''}</div>
                <div class="feed-title">${item.title || 'Berita'}</div>
                <div class="feed-meta">
                    Komoditas: <strong>${item.komoditas || '-'}</strong>
                </div>
            </div>
        `;
    }).join('');

    document.getElementById('liveRssUpdate').textContent = 'Diperbarui: ' + new Date().toLocaleTimeString('id-ID');
}

async function init() {
    const loadingOverlay = document.getElementById('loadingOverlay');

    // Load spark data
    const data = await fetchData();
    if (data) {
        updateSummary(data);
        renderVolatilitasChart(data);
        renderTrenChart(data);
        renderVolatilitasTable(data);
        renderKorelasiTable(data);
    }

    // Load live data
    const liveData = await fetchLiveData();
    if (liveData) {
        renderLiveApi(liveData);
        renderLiveRss(liveData);
    }

    // Hide loading
    if (loadingOverlay) {
        loadingOverlay.classList.add('hidden');
    }
}

// Auto-refresh setiap 30 detik
setInterval(async () => {
    const data = await fetchData();
    if (data) {
        updateSummary(data);
        renderVolatilitasChart(data);
        renderTrenChart(data);
        renderVolatilitasTable(data);
        renderKorelasiTable(data);
    }

    const liveData = await fetchLiveData();
    if (liveData) {
        renderLiveApi(liveData);
        renderLiveRss(liveData);
    }
}, 30000);

// Init on load
document.addEventListener('DOMContentLoaded', init);

