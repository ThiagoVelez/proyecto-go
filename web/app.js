// GopherQueue Frontend Controller
const app = {
  apiBase: '',
  pollInterval: 800,
  workerCount: 4,

  init() {
    this.bindEvents();
    this.startPolling();
    this.fetchData();
  },

  bindEvents() {
    // Single task submission
    const form = document.getElementById('task-form');
    if (form) {
      form.addEventListener('submit', (e) => this.handleTaskSubmit(e));
    }

    // Worker scaling buttons
    document.getElementById('btn-scale-up')?.addEventListener('click', () => {
      this.scaleWorkers(this.workerCount + 1);
    });

    document.getElementById('btn-scale-down')?.addEventListener('click', () => {
      if (this.workerCount > 1) {
        this.scaleWorkers(this.workerCount - 1);
      }
    });
  },

  startPolling() {
    setInterval(() => {
      this.fetchData();
    }, this.pollInterval);
  },

  async fetchData() {
    try {
      await Promise.all([
        this.refreshMetrics(),
        this.refreshWorkers(),
        this.refreshTasks()
      ]);
    } catch (err) {
      console.warn('Polling error:', err);
    }
  },

  async refreshMetrics() {
    const res = await fetch(`${this.apiBase}/api/metrics`);
    if (!res.ok) return;
    const m = await res.json();

    document.getElementById('stat-pending').innerText = m.pending_count;
    document.getElementById('stat-running').innerText = `${m.running_count} (${m.active_workers}w)`;
    document.getElementById('stat-completed').innerText = m.total_completed;
    document.getElementById('stat-failed').innerHTML = `${m.total_failed} <small id="stat-retried">(${m.total_retried} r)</small>`;
    document.getElementById('stat-duration').innerText = `${Math.round(m.avg_duration_ms)} ms`;

    this.workerCount = m.total_workers;
    document.getElementById('header-worker-count').innerText = m.total_workers;
    document.getElementById('worker-pool-title-count').innerText = `${m.active_workers}/${m.total_workers}`;
  },

  async refreshWorkers() {
    const res = await fetch(`${this.apiBase}/api/workers`);
    if (!res.ok) return;
    const workers = await res.json();

    const container = document.getElementById('workers-grid');
    if (!container) return;

    container.innerHTML = workers.map(w => {
      const isBusy = w.status === 'BUSY';
      return `
        <div class="worker-card ${isBusy ? 'is-busy' : ''}">
          <div class="worker-top">
            <span class="worker-title">Worker #${w.id}</span>
            <span class="badge ${isBusy ? 'badge-busy' : 'badge-idle'}">${w.status}</span>
          </div>
          <div class="worker-task-info">
            ${isBusy ? `
              <span style="color: var(--text-muted); font-size: 0.7rem;">Ejecutando:</span>
              <strong style="color: var(--text-primary); display:block;">${w.current_type}</strong>
              <span class="worker-task-id">${w.current_task_id}</span>
            ` : `
              <span style="color: var(--text-muted);">Esperando tareas...</span>
            `}
          </div>
          <div class="worker-stats">
            <span>Completadas:</span>
            <strong>${w.tasks_processed}</strong>
          </div>
        </div>
      `;
    }).join('');
  },

  async refreshTasks() {
    const statusFilter = document.getElementById('filter-status')?.value || '';
    const res = await fetch(`${this.apiBase}/api/tasks?limit=30&status=${statusFilter}`);
    if (!res.ok) return;
    const tasks = await res.json();

    const tbody = document.getElementById('tasks-table-body');
    if (!tbody) return;

    if (!tasks || tasks.length === 0) {
      tbody.innerHTML = `<tr><td colspan="7" class="empty-state">No hay tareas registradas en este estado.</td></tr>`;
      return;
    }

    tbody.innerHTML = tasks.map(t => {
      let priorityClass = 'p-LOW';
      let priorityText = 'LOW';
      if (t.priority === 1) { priorityClass = 'p-HIGH'; priorityText = 'HIGH'; }
      if (t.priority === 2) { priorityClass = 'p-MEDIUM'; priorityText = 'MED'; }

      let infoText = '-';
      if (t.error) {
        infoText = `<span style="color: var(--accent-rose);">${this.escapeHtml(t.error)}</span>`;
      } else if (t.result) {
        infoText = `<code style="font-size: 0.75rem; color: var(--accent-emerald);">${this.escapeHtml(JSON.stringify(t.result))}</code>`;
      }

      return `
        <tr>
          <td class="task-id-cell">${t.id}</td>
          <td><strong>${t.type}</strong></td>
          <td><span class="priority-badge ${priorityClass}">${priorityText}</span></td>
          <td><span class="badge status-${t.status}">${t.status}</span></td>
          <td>${t.current_retries} / ${t.max_retries}</td>
          <td>${t.duration_ms > 0 ? t.duration_ms + ' ms' : '-'}</td>
          <td>${infoText}</td>
        </tr>
      `;
    }).join('');
  },

  async handleTaskSubmit(e) {
    e.preventDefault();
    const type = document.getElementById('task-type').value;
    const priority = parseInt(document.getElementById('task-priority').value, 10);
    const maxRetries = parseInt(document.getElementById('task-retries').value, 10);
    let payload = {};

    try {
      payload = JSON.parse(document.getElementById('task-payload').value);
    } catch (err) {
      alert('El payload debe ser un JSON válido.');
      return;
    }

    try {
      const res = await fetch(`${this.apiBase}/api/tasks`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          type,
          priority,
          payload,
          max_retries: maxRetries
        })
      });

      if (res.ok) {
        this.fetchData();
      } else {
        const err = await res.json();
        alert(`Error al encolar tarea: ${err.error}`);
      }
    } catch (err) {
      alert(`Error de red: ${err.message}`);
    }
  },

  async dispatchBatch(count, taskType) {
    try {
      const res = await fetch(`${this.apiBase}/api/tasks/batch`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          count,
          type: taskType,
          priority: Math.floor(Math.random() * 3) + 1,
          payload: { batch_test: true, timestamp: Date.now() }
        })
      });

      if (res.ok) {
        this.fetchData();
      }
    } catch (err) {
      console.error('Batch dispatch error:', err);
    }
  },

  async scaleWorkers(newCount) {
    try {
      const res = await fetch(`${this.apiBase}/api/workers/scale`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ count: newCount })
      });

      if (res.ok) {
        this.fetchData();
      }
    } catch (err) {
      console.error('Scale error:', err);
    }
  },

  escapeHtml(str) {
    if (typeof str !== 'string') return str;
    return str.replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }
};

window.addEventListener('DOMContentLoaded', () => app.init());
