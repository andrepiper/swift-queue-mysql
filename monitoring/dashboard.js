const express = require('express')
const MysqlBoss = require('../src/index')

class Dashboard {
  constructor (boss, options = {}) {
    this.boss = boss
    this.app = express()
    this.port = options.port || 3000
    this.refreshInterval = options.refreshInterval || 5000
    
    this.setupMiddleware()
    this.setupRoutes()
  }

  setupMiddleware () {
    this.app.use(express.json())
    this.app.use(express.static(__dirname + '/public'))
  }

  setupRoutes () {
    // API Routes
    this.app.get('/api/status', async (req, res) => {
      try {
        const states = await this.boss.countStates()
        const queues = await this.boss.getQueues()
        const wipData = await this.boss.getWipData()
        
        res.json({
          states,
          queues,
          workers: wipData,
          timestamp: new Date().toISOString()
        })
      } catch (error) {
        res.status(500).json({ error: error.message })
      }
    })

    this.app.get('/api/queues', async (req, res) => {
      try {
        const queues = await this.boss.getQueues()
        res.json(queues)
      } catch (error) {
        res.status(500).json({ error: error.message })
      }
    })

    this.app.get('/api/queues/:name', async (req, res) => {
      try {
        const queue = await this.boss.getQueue(req.params.name)
        if (!queue) {
          return res.status(404).json({ error: 'Queue not found' })
        }
        
        const size = await this.boss.getQueueSize(req.params.name)
        res.json({ ...queue, size })
      } catch (error) {
        res.status(500).json({ error: error.message })
      }
    })

    this.app.post('/api/queues/:name/jobs', async (req, res) => {
      try {
        const { data, options } = req.body
        const jobId = await this.boss.send(req.params.name, data, options)
        res.json({ jobId })
      } catch (error) {
        res.status(500).json({ error: error.message })
      }
    })

    this.app.delete('/api/queues/:name', async (req, res) => {
      try {
        await this.boss.purgeQueue(req.params.name)
        res.json({ message: 'Queue purged successfully' })
      } catch (error) {
        res.status(500).json({ error: error.message })
      }
    })

    this.app.post('/api/maintenance', async (req, res) => {
      try {
        const result = await this.boss.maintain()
        res.json(result)
      } catch (error) {
        res.status(500).json({ error: error.message })
      }
    })

    this.app.get('/api/schedules', async (req, res) => {
      try {
        const schedules = await this.boss.getSchedules()
        res.json(schedules)
      } catch (error) {
        res.status(500).json({ error: error.message })
      }
    })

    // Dashboard HTML
    this.app.get('/', (req, res) => {
      res.send(this.getDashboardHTML())
    })
  }

  getDashboardHTML () {
    return `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MySQL Boss Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { background: #333; color: white; padding: 20px; border-radius: 5px; margin-bottom: 20px; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 20px; }
        .stat-card { background: white; padding: 20px; border-radius: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
        .stat-value { font-size: 2em; font-weight: bold; color: #333; }
        .stat-label { color: #666; margin-top: 5px; }
        .queues { background: white; padding: 20px; border-radius: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); margin-bottom: 20px; }
        .queue-item { border-bottom: 1px solid #eee; padding: 15px 0; }
        .queue-name { font-weight: bold; color: #333; }
        .queue-stats { margin-top: 10px; }
        .queue-stat { display: inline-block; margin-right: 20px; }
        .workers { background: white; padding: 20px; border-radius: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
        .worker-item { border-bottom: 1px solid #eee; padding: 15px 0; }
        .status-active { color: #28a745; }
        .status-stopping { color: #ffc107; }
        .status-stopped { color: #dc3545; }
        .btn { padding: 10px 20px; background: #007bff; color: white; border: none; border-radius: 3px; cursor: pointer; }
        .btn:hover { background: #0056b3; }
        .btn-danger { background: #dc3545; }
        .btn-danger:hover { background: #c82333; }
        .actions { margin-bottom: 20px; }
        .loading { text-align: center; padding: 20px; color: #666; }
        .error { color: #dc3545; padding: 10px; background: #f8d7da; border-radius: 3px; margin-bottom: 20px; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>MySQL Boss Dashboard</h1>
            <p>Real-time monitoring and management for MySQL Boss job queues</p>
        </div>
        
        <div class="actions">
            <button class="btn" onclick="runMaintenance()">Run Maintenance</button>
            <button class="btn" onclick="refreshData()">Refresh</button>
        </div>

        <div id="error" class="error" style="display: none;"></div>

        <div class="stats">
            <div class="stat-card">
                <div class="stat-value" id="total-jobs">-</div>
                <div class="stat-label">Total Jobs</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="active-jobs">-</div>
                <div class="stat-label">Active Jobs</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="completed-jobs">-</div>
                <div class="stat-label">Completed Jobs</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="failed-jobs">-</div>
                <div class="stat-label">Failed Jobs</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="queue-count">-</div>
                <div class="stat-label">Queues</div>
            </div>
        </div>

        <div class="queues">
            <h2>Queues</h2>
            <div id="queues-list">
                <div class="loading">Loading queues...</div>
            </div>
        </div>

        <div class="workers">
            <h2>Workers</h2>
            <div id="workers-list">
                <div class="loading">Loading workers...</div>
            </div>
        </div>
    </div>

    <script>
        let refreshTimer;
        
        async function fetchData() {
            try {
                const response = await fetch('/api/status');
                const data = await response.json();
                updateDashboard(data);
                hideError();
            } catch (error) {
                showError('Failed to fetch data: ' + error.message);
            }
        }

        function updateDashboard(data) {
            // Update stats
            document.getElementById('total-jobs').textContent = data.states.all || 0;
            document.getElementById('active-jobs').textContent = data.states.active || 0;
            document.getElementById('completed-jobs').textContent = data.states.completed || 0;
            document.getElementById('failed-jobs').textContent = data.states.failed || 0;
            document.getElementById('queue-count').textContent = data.queues.length;

            // Update queues
            const queuesList = document.getElementById('queues-list');
            if (data.queues.length === 0) {
                queuesList.innerHTML = '<div class="loading">No queues found</div>';
            } else {
                queuesList.innerHTML = data.queues.map(queue => {
                    const queueStates = data.states.queues[queue.name] || {};
                    const totalJobs = Object.values(queueStates).reduce((a, b) => a + b, 0);
                    
                    return \`
                        <div class="queue-item">
                            <div class="queue-name">\${queue.name}</div>
                            <div class="queue-stats">
                                <span class="queue-stat">Total: \${totalJobs}</span>
                                <span class="queue-stat">Active: \${queueStates.active || 0}</span>
                                <span class="queue-stat">Completed: \${queueStates.completed || 0}</span>
                                <span class="queue-stat">Failed: \${queueStates.failed || 0}</span>
                                <span class="queue-stat">Policy: \${queue.policy || 'standard'}</span>
                            </div>
                        </div>
                    \`;
                }).join('');
            }

            // Update workers
            const workersList = document.getElementById('workers-list');
            if (data.workers.length === 0) {
                workersList.innerHTML = '<div class="loading">No active workers</div>';
            } else {
                workersList.innerHTML = data.workers.map(worker => \`
                    <div class="worker-item">
                        <div class="queue-name">\${worker.name}</div>
                        <div class="queue-stats">
                            <span class="queue-stat">State: <span class="status-\${worker.state}">\${worker.state}</span></span>
                            <span class="queue-stat">Started: \${new Date(worker.createdOn).toLocaleString()}</span>
                            \${worker.lastJobStartedOn ? \`<span class="queue-stat">Last Job: \${new Date(worker.lastJobStartedOn).toLocaleString()}</span>\` : ''}
                        </div>
                    </div>
                \`).join('');
            }
        }

        function showError(message) {
            const errorDiv = document.getElementById('error');
            errorDiv.textContent = message;
            errorDiv.style.display = 'block';
        }

        function hideError() {
            document.getElementById('error').style.display = 'none';
        }

        async function runMaintenance() {
            try {
                const response = await fetch('/api/maintenance', { method: 'POST' });
                const result = await response.json();
                alert(\`Maintenance completed:
Expired jobs: \${result.expiredJobs}
Archived jobs: \${result.archivedJobs}
Deleted jobs: \${result.deletedJobs}\`);
                refreshData();
            } catch (error) {
                showError('Failed to run maintenance: ' + error.message);
            }
        }

        function refreshData() {
            fetchData();
        }

        function startAutoRefresh() {
            refreshTimer = setInterval(fetchData, ${this.refreshInterval});
        }

        function stopAutoRefresh() {
            if (refreshTimer) {
                clearInterval(refreshTimer);
            }
        }

        // Initialize
        document.addEventListener('DOMContentLoaded', function() {
            fetchData();
            startAutoRefresh();
        });

        // Cleanup on page unload
        window.addEventListener('beforeunload', function() {
            stopAutoRefresh();
        });
    </script>
</body>
</html>
    `
  }

  start () {
    return new Promise((resolve, reject) => {
      this.server = this.app.listen(this.port, (error) => {
        if (error) {
          reject(error)
        } else {
          console.log(`MySQL Boss Dashboard running on http://localhost:${this.port}`)
          resolve()
        }
      })
    })
  }

  stop () {
    return new Promise((resolve) => {
      if (this.server) {
        this.server.close(resolve)
      } else {
        resolve()
      }
    })
  }
}

module.exports = Dashboard
