const express = require('express');
const Docker = require('dockerode');
const cors = require('cors');
const path = require('path');
const { exec } = require('child_process');
const { promisify } = require('util');
const fs = require('fs').promises;
const { Client } = require('ssh2');

const execPromise = promisify(exec);

const app = express();
app.use(cors());
app.use(express.json());

const MAX_LOG_ROWS = 10000;

// Store Docker connections for each node
const dockerConnections = new Map();
const connectionTimestamps = new Map(); // Track when connections were created
const CONNECTION_TIMEOUT = 5 * 60 * 1000; // 5 minutes - reconnect after this
const nodeConfigs = [];
const nodeLocks = new Map(); // Serialize per-node operations to avoid channel exhaustion


// Connection health tracking
const connectionHealth = new Map();
const MAX_FAILURES = 3;
const CIRCUIT_RESET_TIME = 30000; // 30s
const DEFAULT_LOCAL_NODE_NAME = 'Local Node';

// Release cached connection for a node (also ends SSH session if present)
function dropConnection(nodeName, reason = 'unknown') {
  const existing = dockerConnections.get(nodeName);
  if (existing && existing.__sshConn) {
    console.log(`Closing SSH connection to ${nodeName} - reason: ${reason}`);
    try { existing.__sshConn.end(); } catch (err) { /* ignore */ }
  }
  dockerConnections.delete(nodeName);
  connectionTimestamps.delete(nodeName);
}

// Circuit breaker: track node health
function recordFailure(nodeName) {
  const health = connectionHealth.get(nodeName) || { failCount: 0, lastFail: 0, circuitOpen: false };
  health.failCount++;
  health.lastFail = Date.now();
  
  if (health.failCount >= MAX_FAILURES) {
    health.circuitOpen = true;
    console.warn(`Circuit breaker opened for ${nodeName} after ${health.failCount} failures`);
    setTimeout(() => {
      health.circuitOpen = false;
      health.failCount = 0;
      console.log(`Circuit breaker reset for ${nodeName}`);
    }, CIRCUIT_RESET_TIME);
  }
  
  connectionHealth.set(nodeName, health);
}

function recordSuccess(nodeName) {
  const health = connectionHealth.get(nodeName);
  if (health && health.failCount > 0) {
    health.failCount = Math.max(0, health.failCount - 1);
  }
}

function isCircuitOpen(nodeName) {
  const health = connectionHealth.get(nodeName);
  return health && health.circuitOpen;
}

// Non-blocking operation executor with circuit breaker
function runWithNodeLock(nodeName, fn) {
  // Check circuit breaker
  if (isCircuitOpen(nodeName)) {
    return Promise.reject(new Error(`Service temporarily unavailable for ${nodeName}`));
  }
  
  const cfg = nodeConfigs.find(n => n.name === nodeName);
  const timeoutMs = cfg?.type === 'local' ? 30000 : 45000; // 45s for SSH to allow task fetching
  
  // Execute without waiting for locks - better concurrency
  return (async () => {
    try {
      const result = await Promise.race([
        fn(),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error(`Operation timeout on node ${nodeName}`)), timeoutMs)
        )
      ]);
      
      recordSuccess(nodeName);
      return result;
    } catch (err) {
      // Only drop SSH connections on critical errors
      if (cfg?.type === 'ssh' && 
          err.message !== 'Request aborted' && 
          (err.message.includes('Not connected') || err.message.includes('Connection refused'))) {
        console.error(`SSH connection error for ${nodeName}:`, err.message);
        dropConnection(nodeName, err.message);
        recordFailure(nodeName);
      }
      throw err;
    }
  })();
}

// Load node configurations from secrets/config
async function loadNodeConfigs() {
  try {
    const configPath = '/run/secrets/nodes_config';
    let configData;
    
    try {
      configData = await fs.readFile(configPath, 'utf8');
    } catch (err) {
      console.log('No nodes_config secret found, using local-only configuration');
      configData = '[]';
    }

    const parsedConfigs = JSON.parse(configData);
    if (!Array.isArray(parsedConfigs)) {
      throw new Error('nodes_config must be a JSON array');
    }

    const remoteConfigs = parsedConfigs
      .filter(config => config && typeof config === 'object')
      .filter(config =>
        typeof config.name === 'string' && config.name.trim().length > 0 &&
        typeof config.host === 'string' && config.host.trim().length > 0 &&
        typeof config.username === 'string' && config.username.trim().length > 0 &&
        typeof config.password === 'string' && config.password.length > 0
      )
      .map(config => ({
        name: config.name.trim(),
        type: 'ssh',
        host: config.host.trim(),
        port: 22,
        username: config.username.trim(),
        password: config.password
      }));

    const usedNames = new Set(remoteConfigs.map(config => config.name));
    let localName = DEFAULT_LOCAL_NODE_NAME;
    if (usedNames.has(localName)) {
      let suffix = 1;
      while (usedNames.has(`${localName}-${suffix}`)) {
        suffix++;
      }
      localName = `${localName}-${suffix}`;
    }

    const localConfig = {
      name: localName,
      type: 'local',
      host: 'localhost'
    };

    nodeConfigs.length = 0;
    nodeConfigs.push(localConfig, ...remoteConfigs);
    
    for (const config of nodeConfigs) {
      if (config.type === 'local') {
        dockerConnections.set(config.name, new Docker({ socketPath: '/var/run/docker.sock' }));
        connectionTimestamps.set(config.name, Date.now());
        console.log(`✓ Connected to local node: ${config.name}`);
      } else if (config.type === 'ssh') {
        console.log(`✓ Configured SSH node: ${config.name} (${config.host})`);
      }
    }
    
    console.log(`Loaded ${nodeConfigs.length} node configuration(s)`);
  } catch (error) {
    console.error('Error loading node configs:', error.message);
    nodeConfigs.length = 0;
    nodeConfigs.push({
      name: DEFAULT_LOCAL_NODE_NAME,
      type: 'local',
      host: 'localhost'
    });
    dockerConnections.set(DEFAULT_LOCAL_NODE_NAME, new Docker({ socketPath: '/var/run/docker.sock' }));
  }
}

// Get Docker connection for a node with retry logic
async function getDockerConnection(nodeName) {
  const config = nodeConfigs.find(n => n.name === nodeName);
  if (!config) {
    throw new Error(`Node ${nodeName} not found`);
  }

  // Local node: always use socket, never expire
  if (config.type === 'local') {
    if (dockerConnections.has(nodeName)) {
      return dockerConnections.get(nodeName);
    }
    const docker = new Docker({ socketPath: '/var/run/docker.sock' });
    dockerConnections.set(nodeName, docker);
    connectionTimestamps.set(nodeName, Date.now());
    return docker;
  }

  // SSH node: check if circuit is open
  if (isCircuitOpen(nodeName)) {
    throw new Error(`Connection to ${nodeName} is temporarily disabled`);
  }

  // SSH node: reuse if fresh and healthy
  if (dockerConnections.has(nodeName)) {
    const createdAt = connectionTimestamps.get(nodeName) || 0;
    const age = Date.now() - createdAt;
    if (age < CONNECTION_TIMEOUT) {
      const conn = dockerConnections.get(nodeName);
      // Quick health check
      if (conn.__sshConn && conn.__sshConn._sock && !conn.__sshConn._sock.destroyed) {
        return conn;
      } else {
        console.log(`SSH connection to ${nodeName} is dead, reconnecting...`);
        dropConnection(nodeName, 'socket destroyed');
      }
    } else {
      console.log(`Removing stale SSH connection to ${nodeName} (age: ${age}ms)`);
      dropConnection(nodeName, 'stale');
    }
  }

  const docker = await createSSHDockerConnection(config);
  dockerConnections.set(nodeName, docker);
  connectionTimestamps.set(nodeName, Date.now());
  return docker;
}

// Create Docker connection via SSH
async function createSSHDockerConnection(config) {
  return new Promise(async (resolve, reject) => {
    const conn = new Client();
    let connectionTimeout;
    let isConnected = false;
    
    const password = typeof config.password === 'string' ? config.password : '';
    if (!password) {
      return reject(new Error(`SSH password is missing for node ${config.name}`));
    }
    
    // Set connection timeout (15 seconds)
    connectionTimeout = setTimeout(() => {
      conn.end();
      reject(new Error(`SSH connection timeout to ${config.host}`));
    }, 15000);
    
    conn.on('ready', () => {
      clearTimeout(connectionTimeout);
      isConnected = true;
      console.log(`SSH connection established to ${config.host}`);
      conn.exec('docker version', (err, stream) => {
        if (err) {
          console.error(`Docker version check failed on ${config.host}:`, err.message);
          conn.end();
          return reject(new Error(`Docker not available on ${config.host}: ${err.message}`));
        }
        const docker = createSSHDockerode(conn, config);
        resolve(docker);
      });
    });
    
    conn.on('error', (err) => {
      clearTimeout(connectionTimeout);
      console.error(`SSH connection error to ${config.host}:`, err.message);
      // Don't drop on every error - let circuit breaker handle it
      recordFailure(config.name);
      reject(new Error(`SSH connection failed to ${config.host}: ${err.message}`));
    });
    
    // Close handler
    conn.on('close', () => {
      clearTimeout(connectionTimeout);
      if (isConnected) {
        console.log(`SSH connection closed to ${config.host}`);
        // Remove stale connection from cache
        dockerConnections.delete(config.name);
        connectionTimestamps.delete(config.name);
      }
    });
    
    conn.connect({
      host: config.host,
      port: config.port || 22,
      username: config.username || 'root',
      password: password,
      readyTimeout: 15000,
      algorithms: {
        serverHostKey: ['ssh-rsa', 'ssh-dss', 'ecdsa-sha2-nistp256']
      }
    });
  });
}

// Create Dockerode-like wrapper for SSH commands
function createSSHDockerode(sshConn, config) {
  const commandQueue = [];
  let isProcessingQueue = false;
  
  // Process commands one at a time to prevent SSH channel exhaustion
  const processQueue = async () => {
    if (isProcessingQueue || commandQueue.length === 0) return;
    
    isProcessingQueue = true;
    while (commandQueue.length > 0) {
      const { cmd, resolve, reject, timeoutMs } = commandQueue.shift();
      try {
        const result = await executeSSHCommand(cmd, timeoutMs);
        resolve(result);
      } catch (err) {
        reject(err);
      }
      // Small delay between commands
      await new Promise(r => setTimeout(r, 50));
    }
    isProcessingQueue = false;
  };

  const DEFAULT_SSH_CMD_TIMEOUT = 10000;
  const execSSHCommand = (cmd, timeoutMs = DEFAULT_SSH_CMD_TIMEOUT) => {
    return new Promise((resolve, reject) => {
      console.log(`[SSH:${config.name}] Queueing command: ${cmd.substring(0, 100)}...`);
      commandQueue.push({ cmd, resolve, reject, timeoutMs });
      processQueue();
    });
  };

  const executeSSHCommand = (cmd, timeoutMs = DEFAULT_SSH_CMD_TIMEOUT) => {
    return new Promise((resolve, reject) => {
      console.log(`[SSH:${config.name}] Executing command: ${cmd.substring(0, 100)}...`);
      // Check if connection is still alive
      if (!sshConn || !sshConn._sock || sshConn._sock.destroyed) {
        console.error(`[SSH:${config.name}] Connection is closed!`);
        return reject(new Error('SSH connection is closed'));
      }

      let isResolved = false;

      // Per-command timeout (configurable; large log fetches get a longer budget)
      const cmdTimeout = setTimeout(() => {
        if (!isResolved) {
          console.error(`[SSH:${config.name}] Command timeout after ${timeoutMs}ms`);
          isResolved = true;
          reject(new Error(`SSH command timeout: ${cmd.substring(0, 50)}`));
        }
      }, timeoutMs);
      
      console.log(`[SSH:${config.name}] Calling sshConn.exec...`);
      sshConn.exec(cmd, (err, stream) => {
        if (isResolved) return;
        
        if (err) {
          clearTimeout(cmdTimeout);
          console.error(`[SSH:${config.name}] exec() error:`, err.message);
          if (!isResolved) {
            isResolved = true;
            // Channel open failure means connection is bad - drop it
            if (err.message && err.message.includes('Channel open failure')) {
              console.error(`[SSH:${config.name}] Channel open failure detected, dropping connection`);
              dropConnection(config.name, 'channel-failure');
            }
            reject(err);
          }
          return;
        }
        
        console.log(`[SSH:${config.name}] Stream opened, collecting data...`);
        let stdout = '';
        let stderr = '';
        
        stream.on('data', (data) => {
          stdout += data.toString();
        });
        
        stream.stderr.on('data', (data) => {
          stderr += data.toString();
        });
        
        stream.on('close', (code) => {
          clearTimeout(cmdTimeout);
          console.log(`[SSH:${config.name}] Stream closed with code: ${code}, stdout length: ${stdout.length}, stderr length: ${stderr.length}`);
          if (isResolved) return;
          isResolved = true;
          
          // For docker service logs commands, even with warnings in stderr, we want the stdout
          // Exit code 1 with "incomplete log stream" warning should still return stdout
          const isServiceLogsWarning = stderr.includes('incomplete log stream') || stderr.includes('some logs could not be retrieved');
          
          if (code !== 0 && !isServiceLogsWarning) {
            console.error(`[SSH:${config.name}] Command failed with code ${code}, stderr: ${stderr.substring(0, 200)}`);
            reject(new Error(stderr || `Command failed with code ${code}`));
          } else {
            if (isServiceLogsWarning) {
              console.log(`[SSH:${config.name}] Warning in stderr (but continuing): ${stderr.substring(0, 150)}`);
            }
            console.log(`[SSH:${config.name}] Command succeeded, returning ${stdout.length} bytes`);
            resolve(stdout);
          }
        });
        
        stream.on('error', (err) => {
          clearTimeout(cmdTimeout);
          console.error(`[SSH:${config.name}] Stream error:`, err.message);
          if (!isResolved) {
            isResolved = true;
            reject(err);
          }
        });
      });
    });
  };

  // Helper function to parse relative time strings like "Failed 2 months ago" into actual timestamps
  const parseRelativeTime = (stateStr) => {
    if (!stateStr) return new Date();
    
    // Extract time portion: "Failed 2 months ago" -> "2 months ago"
    const match = stateStr.match(/(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago/i);
    if (!match) {
      // If no time found (e.g., just "Running" or "Shutdown"), assume now
      return new Date();
    }
    
    const amount = parseInt(match[1], 10);
    const unit = match[2].toLowerCase();
    const now = new Date();
    
    switch (unit) {
      case 'second':
        now.setSeconds(now.getSeconds() - amount);
        break;
      case 'minute':
        now.setMinutes(now.getMinutes() - amount);
        break;
      case 'hour':
        now.setHours(now.getHours() - amount);
        break;
      case 'day':
        now.setDate(now.getDate() - amount);
        break;
      case 'week':
        now.setDate(now.getDate() - (amount * 7));
        break;
      case 'month':
        now.setMonth(now.getMonth() - amount);
        break;
      case 'year':
        now.setFullYear(now.getFullYear() - amount);
        break;
    }
    
    return now;
  };

  // ----- Swarm-role awareness + manager delegation --------------------------
  // Swarm query commands (service ls/inspect/ps/logs, node ls) only work on
  // managers. When the selected node is a worker, we don't reconstruct anything
  // locally — we SSH to a MANAGER of the same swarm and run the real commands
  // there. A manager has the full cluster view, so it can list, inspect, log and
  // control the worker's tasks too. Only host-level usage stays on the worker.
  let managerStatusCache = null;
  const isManagerNode = async () => {
    if (managerStatusCache !== null) return managerStatusCache;
    try {
      const out = (await execSSHCommand("docker info --format '{{.Swarm.ControlAvailable}}'")).trim();
      managerStatusCache = out === 'true';
    } catch (err) {
      // Assume manager on error so healthy manager nodes are never disturbed.
      managerStatusCache = true;
    }
    return managerStatusCache;
  };

  let clusterIdCache = null;
  const getClusterId = async () => {
    if (clusterIdCache !== null) return clusterIdCache;
    try {
      clusterIdCache = (await execSSHCommand("docker info --format '{{.Swarm.Cluster.ID}}'")).trim();
    } catch (err) {
      clusterIdCache = '';
    }
    return clusterIdCache;
  };

  const isConnAlive = (d) => d && d.__sshConn && d.__sshConn._sock && !d.__sshConn._sock.destroyed;

  // Resolve a docker connection to a swarm MANAGER for this node's swarm.
  // Returns this wrapper itself when this node is already a manager.
  let managerDelegate = null;
  const getManagerDelegate = async () => {
    if (await isManagerNode()) return dockerApi;
    if (isConnAlive(managerDelegate)) return managerDelegate;
    managerDelegate = null;

    const myCluster = await getClusterId();

    // 1) Prefer an already-configured SSH node that is a manager of the SAME swarm.
    for (const n of nodeConfigs) {
      if (n.type !== 'ssh' || n.name === config.name) continue;
      try {
        const cand = await getDockerConnection(n.name);
        if (!(await cand.isManager())) continue;
        const candCluster = (await cand.execHost("docker info --format '{{.Swarm.Cluster.ID}}'")).trim();
        if (myCluster && candCluster && candCluster !== myCluster) continue; // different swarm
        managerDelegate = cand;
        console.log(`[delegate] worker ${config.name} -> configured manager ${n.name}`);
        return managerDelegate;
      } catch (err) { /* try next candidate */ }
    }

    // 2) Fall back to a manager IP this node advertises, reusing its SSH creds.
    try {
      const raw = (await execSSHCommand("docker info --format '{{json .Swarm.RemoteManagers}}'")).trim();
      const managers = JSON.parse(raw || 'null') || [];
      for (const m of managers) {
        const ip = String(m.Addr || '').split(':')[0];
        if (!ip) continue;
        try {
          const cand = await createSSHDockerConnection({ ...config, host: ip, name: `${config.name}::mgr(${ip})` });
          if (await cand.isManager()) {
            managerDelegate = cand;
            console.log(`[delegate] worker ${config.name} -> manager ${ip} (via RemoteManagers)`);
            return managerDelegate;
          }
          try { cand.__sshConn.end(); } catch (e) { /* ignore */ }
        } catch (err) { /* try next */ }
      }
    } catch (err) { /* ignore */ }

    throw new Error(`No reachable swarm manager found for worker node ${config.name}`);
  };

  const dockerApi = {
    async listServices() {
      // Worker: delegate to a manager (full swarm view).
      const swarm = await getManagerDelegate();
      if (swarm !== dockerApi) return swarm.listServices();

      const output = await execSSHCommand('docker service ls --format "{{json .}}"');
      const lines = output.trim().split('\n').filter(l => l);
      return lines.map(line => {
        const data = JSON.parse(line);
        const repStr = data.Replicas || '';
        const parts = repStr.split('/');
        const currentRep = parseInt(parts[0]) || 0;
        const desiredRep = parseInt(parts[1]) || 0;
        return {
          ID: data.ID,
          Spec: {
            Name: data.Name,
            Mode: data.Mode.includes('replicated') ? { Replicated: {} } : { Global: {} },
            TaskTemplate: { ContainerSpec: { Image: data.Image } }
          },
          ReplicasParsed: { current: currentRep, desired: desiredRep }
        };
      });
    },

    async listTasks(options) {
      const serviceId = options?.filters?.service?.[0];
      const skipContainerLookup = options?.skipContainerLookup === true;

      // Worker: delegate to a manager (sees tasks across all nodes).
      const swarm = await getManagerDelegate();
      if (swarm !== dockerApi) return swarm.listTasks(options);

      const cmd = serviceId
        ? `docker service ps ${serviceId} --format "{{json .}}" --no-trunc`
        : 'docker ps --format "{{json .}}"';

      const output = await execSSHCommand(cmd);
      const lines = output.trim().split('\n').filter(l => l);
      
      const tasks = lines.map(line => {
        const data = JSON.parse(line);
        // Parse CurrentState field: format is "Running 10 seconds ago" or "Failed 5 minutes ago" or "Shutdown"
        const currentState = data.CurrentState || '';
        const currentStateLower = currentState.toLowerCase();
        
        // Parse Error field if present (contains actual error message for failed tasks)
        const errorMessage = data.Error || '';
        
        let state = 'pending';
        if (currentStateLower.startsWith('running')) state = 'running';
        else if (currentStateLower.startsWith('failed')) state = 'failed';
        else if (currentStateLower.startsWith('shutdown')) state = 'shutdown';
        else if (currentStateLower.startsWith('complete')) state = 'complete';
        else if (currentStateLower.startsWith('rejected')) state = 'rejected';
        else if (currentStateLower.startsWith('preparing')) state = 'preparing';
        else if (currentStateLower.startsWith('starting')) state = 'starting';
        
        // Parse relative time from CurrentState to get actual timestamp
        const parsedTime = parseRelativeTime(currentState);
        
        return {
          ID: data.ID,
          NodeID: data.Node || '',
          Status: {
            State: state,
            Err: errorMessage || null,
            Message: currentState,
            ContainerStatus: { ContainerID: '', State: currentState }
          },
          DesiredState: data.DesiredState?.toLowerCase() || 'running',
          UpdatedAt: parsedTime.toISOString()
        };
      });
      
      // Fetch container IDs only if needed (skip for messages endpoint to be faster)
      if (!skipContainerLookup) {
        for (const task of tasks) {
          try {
            // Try to get container ID by inspecting the task
            const taskId = task.ID;
            // First try: use task ID directly with docker inspect
            let inspectCmd = `docker inspect --format '{{.Status.ContainerStatus.ContainerID}}' ${taskId} 2>/dev/null`;
            let containerId = (await execSSHCommand(inspectCmd)).trim();
            
            // If that fails, try docker ps with task name filter (including stopped containers)
            if (!containerId || containerId.length < 10) {
              const taskShortId = taskId.substring(0, 12);
              const psCmd = `docker ps -a --filter "name=${taskShortId}" --format "{{.ID}}" --no-trunc | head -1`;
              containerId = (await execSSHCommand(psCmd)).trim();
            }
            
            if (containerId && containerId.length > 10) {
              task.Status.ContainerStatus.ContainerID = containerId;
            }
          } catch (err) {
            // Silent fail - container lookup failed
          }
        }
      }
      
      return tasks;
    },
    
    async listNodes() {
      // Worker: delegate to a manager (full node list).
      const swarm = await getManagerDelegate();
      if (swarm !== dockerApi) return swarm.listNodes();

      const output = await execSSHCommand('docker node ls --format "{{json .}}"');
      const lines = output.trim().split('\n').filter(l => l);
      return lines.map(line => {
        const data = JSON.parse(line);
        return {
          ID: data.ID,
          Description: { Hostname: data.Hostname },
          Status: { State: data.Status },
          Spec: {
            Availability: data.Availability,
            Role: data.ManagerStatus ? 'manager' : 'worker'
          }
        };
      });
    },

    getService(serviceId) {
      return {
        async inspect() {
          // Worker: delegate to a manager.
          const swarm = await getManagerDelegate();
          if (swarm !== dockerApi) return swarm.getService(serviceId).inspect();

          const output = await execSSHCommand(`docker service inspect ${serviceId}`);
          const data = JSON.parse(output);
          return data[0];
        }
      };
    },
    
    getContainer(containerId) {
      return {
        async logs(options) {
          let cmd = `docker logs ${containerId}`;
          if (options.timestamps) cmd += ' --timestamps';
          if (options.tail) cmd += ` --tail ${options.tail}`;
          if (options.since) cmd += ` --since ${options.since}`;
          if (options.until) cmd += ` --until ${options.until}`;
          
          const output = await execSSHCommand(cmd);
          return output;
        },
        
        async stats(options) {
          const output = await execSSHCommand(`docker stats ${containerId} --no-stream --format "{{json .}}"`);
          const data = JSON.parse(output);
          
          const cpuPercent = parseFloat(data.CPUPerc?.replace('%', '')) || 0;
          const memPercent = parseFloat(data.MemPerc?.replace('%', '')) || 0;
          
          return {
            cpu_stats: {
              cpu_usage: { total_usage: cpuPercent * 10000000 },
              system_cpu_usage: 100 * 10000000,
              online_cpus: 1
            },
            memory_stats: {
              usage: memPercent * 10000000,
              limit: 100 * 10000000
            }
          };
        }
      };
    },
    // Execute a host-level command on the SSH node
    async execHost(cmd, timeoutMs) {
      const out = await execSSHCommand(cmd, timeoutMs);
      return out;
    },

    // Whether this node is a swarm manager (cached for the connection lifetime).
    async isManager() {
      return isManagerNode();
    },

    // A docker connection to a swarm MANAGER for this node's swarm (self if this
    // node is already a manager; otherwise a configured/discovered manager). All
    // swarm-only operations should run against this so worker nodes work too.
    async getSwarmDocker() {
      return getManagerDelegate();
    }
  };

  // Keep reference to SSH connection for cleanup
  dockerApi.__sshConn = sshConn;
  return dockerApi;
}

// Health check endpoint
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', message: 'Docker Swarm Monitor API is running' });
});

// Get list of available nodes
app.get('/api/nodes-list', async (req, res) => {
  try {
    const nodes = nodeConfigs.map(config => ({
      name: config.name,
      host: config.host,
      type: config.type
    }));
    res.json(nodes);
  } catch (error) {
    console.error('Error fetching nodes list:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get unhealthy services across all nodes (for messages section)
// Cache for error messages to prevent flickering
const errorMessageCache = new Map(); // { 'nodeName:serviceId': { errorMessage, running, desired } }

// Cache for the entire messages response - return instantly
let messagesCache = { data: [], timestamp: 0 };
const MESSAGES_CACHE_TTL = 5000; // 5 seconds - return cached data if fresh
let isRefreshingMessages = false;

app.get('/api/messages', async (req, res) => {
  const now = Date.now();
  const cacheAge = now - messagesCache.timestamp;
  
  // ALWAYS return immediately - never block the page load
  // If cache exists (even stale), return it
  if (messagesCache.timestamp > 0) {
    // Trigger background refresh if cache is stale
    if (cacheAge > MESSAGES_CACHE_TTL && !isRefreshingMessages) {
      refreshMessagesInBackground();
    }
    return res.json(messagesCache.data);
  }
  
  // No cache exists - return empty array immediately and refresh in background
  if (!isRefreshingMessages) {
    refreshMessagesInBackground();
  }
  return res.json([]);
});

// Background refresh function
async function refreshMessagesInBackground() {
  if (isRefreshingMessages) return;
  isRefreshingMessages = true;
  try {
    const messages = await fetchMessagesFromNodes();
    messagesCache = { data: messages, timestamp: Date.now() };
  } catch (err) {
    console.error('Background messages refresh error:', err.message);
  } finally {
    isRefreshingMessages = false;
  }
}

// Actual fetch logic
async function fetchMessagesFromNodes() {
  const globalAbortController = new AbortController();
  const abortTimeoutId = setTimeout(() => {
    globalAbortController.abort();
  }, 15000); // 15s hard timeout
  
  const messages = [];
  
  try {
    // Use Promise.allSettled so one slow/hanging node doesn't block others
    await Promise.allSettled(
      nodeConfigs.map(config =>
        runWithNodeLock(config.name, async () => {
          if (globalAbortController.signal.aborted) throw new Error('Request aborted');
          
          try {
            const docker = await getDockerConnection(config.name);
            const services = await docker.listServices();
            const isSSH = config.type === 'ssh';
            
            for (const service of services) {
              if (globalAbortController.signal.aborted) throw new Error('Request aborted');
              
              const serviceInfo = service.inspect ? await service.inspect() : service;
              let runningTasks = 0;
              let desiredTasks = serviceInfo.Spec.Mode.Replicated?.Replicas || 0;
              let isHealthy = false;
              let tasks = [];
              
              if (isSSH) {
                runningTasks = serviceInfo.ReplicasParsed?.current || 0;
                desiredTasks = serviceInfo.ReplicasParsed?.desired ?? desiredTasks;
                isHealthy = desiredTasks > 0 && runningTasks >= desiredTasks;
              } else {
                // Fetch tasks once for local nodes
                tasks = await docker.listTasks({ filters: { service: [serviceInfo.ID] } });
                runningTasks = tasks.filter(t => t.Status.State === 'running').length;
                const effectiveDesired = (serviceInfo.Spec.Mode.Replicated?.Replicas
                  ?? serviceInfo.ServiceStatus?.DesiredTasks
                  ?? tasks.filter(t => t.DesiredState === 'running').length
                  ?? 0);
                isHealthy = effectiveDesired > 0 && runningTasks >= effectiveDesired;
                desiredTasks = effectiveDesired;
              }
              
              // If service is healthy, clear its error cache
              if (isHealthy) {
                const cacheKey = `${config.name}:${serviceInfo.ID}`;
                errorMessageCache.delete(cacheKey);
              }
              
              if (!isHealthy) {
                let taskState = null;
                let errorMessage = null;
                
                // Get error message from failed tasks
                try {
                  // Fetch tasks only for unhealthy services with a timeout (skip container lookup for speed)
                  if (isSSH && tasks.length === 0) {
                    try {
                      tasks = await Promise.race([
                        docker.listTasks({ filters: { service: [serviceInfo.ID] }, skipContainerLookup: true }),
                        new Promise((_, reject) => setTimeout(() => reject(new Error('Task fetch timeout')), 5000))
                      ]);
                    } catch (taskFetchErr) {
                      console.log(`Task fetch timeout for ${serviceInfo.Spec?.Name || 'unknown'}`);
                      tasks = [];
                    }
                  }
                  
                  if (tasks.length === 0) {
                    // No tasks fetched
                    taskState = 'unknown';
                    errorMessage = 'No tasks found';
                  } else {
                    // Docker returns tasks already sorted by newest first
                    // The first task in the list is the most recent one
                    const latestTask = tasks[0];
                    taskState = latestTask?.Status?.State || 'unknown';
                    
                    console.log(`[DEBUG] ${serviceInfo.Spec?.Name}: Latest task state = ${taskState}, Error = ${latestTask?.Status?.Err || 'none'}`);
                    
                    if (taskState === 'failed' || taskState === 'rejected') {
                      // Latest task is failed - show its error message
                      errorMessage = latestTask.Status?.Err || latestTask.Status?.Message || 'Task failed';
                    } else if (taskState === 'shutdown') {
                      // Latest task is shutdown - no error message needed
                      errorMessage = null;
                    } else if (taskState === 'running') {
                      // Latest task is running but service is still unhealthy (maybe not enough replicas)
                      errorMessage = 'Not enough running replicas';
                    } else {
                      // Other states (preparing, starting, pending, etc.)
                      errorMessage = null;
                    }
                  }
                } catch (taskErr) {
                  console.error(`Error processing tasks for ${serviceInfo.Spec?.Name || 'unknown'}:`, taskErr.message);
                  taskState = 'error';
                  errorMessage = 'Unable to retrieve task status';
                }
                
                // Use cached data to prevent flickering
                // Only update cache if: no cache exists OR running/desired count changed
                const cacheKey = `${config.name}:${serviceInfo.ID}`;
                const cached = errorMessageCache.get(cacheKey);
                
                if (cached) {
                  // If running/desired count is the same, ALWAYS use cached data (no TTL)
                  // This keeps the error message stable as long as service state doesn't change
                  if (cached.running === runningTasks && cached.desired === desiredTasks) {
                    taskState = cached.taskState;
                    errorMessage = cached.errorMessage;
                  } else {
                    // State actually changed (e.g., replicas changed) - update cache
                    errorMessageCache.set(cacheKey, { taskState, errorMessage, running: runningTasks, desired: desiredTasks });
                  }
                } else {
                  // First time seeing this unhealthy service - cache the data
                  errorMessageCache.set(cacheKey, { taskState, errorMessage, running: runningTasks, desired: desiredTasks });
                }
                
                messages.push({
                  nodeName: config.name,
                  serviceId: serviceInfo.ID,
                  serviceName: serviceInfo.Spec.Name,
                  status: 'unhealthy',
                  running: runningTasks,
                  desired: desiredTasks,
                  taskState: taskState,
                  errorMessage: errorMessage,
                  timestamp: new Date().toISOString()
                });
              }
            }
          } catch (err) {
            if (err.message !== 'Request aborted') {
              console.error(`Error checking services on ${config.name}:`, err.message);
              dropConnection(config.name);
              messages.push({
                nodeName: config.name,
                status: 'error',
                error: `Unable to connect: ${err.message}`,
                timestamp: new Date().toISOString()
              });
            }
          }
        })
      )
    );
    
    clearTimeout(abortTimeoutId);
    return messages;
  } catch (error) {
    clearTimeout(abortTimeoutId);
    console.error('Error fetching messages:', error);
    return [];
  }
}

// Cache for services list
const servicesCache = new Map(); // { nodeName: { timestamp, data } }
const SERVICES_CACHE_TTL = 3000; // 3 seconds

// Get all services for a specific node
app.get('/api/services', async (req, res) => {
  const { nodeName } = req.query;
  if (!nodeName) {
    return res.status(400).json({ error: 'nodeName parameter is required' });
  }
  
  // Check cache first - return immediately if fresh
  const cached = servicesCache.get(nodeName);
  const now = Date.now();
  if (cached && (now - cached.timestamp) < SERVICES_CACHE_TTL) {
    return res.json(cached.data);
  }
  
  const config = nodeConfigs.find(n => n.name === nodeName);
  const isSSH = config?.type === 'ssh';
  const timeoutMs = isSSH ? 12000 : 20000; // Allow more time for local aggregation

  const nodeAbortController = new AbortController();
  const abortTimeoutId = setTimeout(() => {
    nodeAbortController.abort();
  }, timeoutMs);
  
  try {
    const servicesWithStatus = await runWithNodeLock(nodeName, async () => {
      if (nodeAbortController.signal.aborted) throw new Error('Request aborted');
      
      try {
        const docker = await getDockerConnection(nodeName);
        const services = await docker.listServices();
        
        const servicesWithStatusInner = [];
        for (const service of services) {
          if (nodeAbortController.signal.aborted) throw new Error('Request aborted');
          
          const serviceInfo = service.inspect ? await service.inspect() : service;
          
          let runningTasks = 0;
          let totalTasks = 0;
          let desiredTasks = serviceInfo.Spec.Mode.Replicated?.Replicas ?? 0;
          let tasks = [];
          
          // For SSH nodes, use replicas from service ls to avoid expensive ps calls
          if (isSSH) {
            runningTasks = serviceInfo.ReplicasParsed?.current || 0;
            desiredTasks = serviceInfo.ReplicasParsed?.desired ?? desiredTasks;
          } else {
            tasks = await docker.listTasks({ filters: { service: [serviceInfo.ID] } });
            runningTasks = tasks.filter(t => t.Status.State === 'running').length;
            totalTasks = tasks.length;
            // Handle global mode: desired is how many tasks are expected to run
            if (serviceInfo.Spec.Mode.Global) {
              const desiredFromTasks = tasks.filter(t => (t.DesiredState || 'running') === 'running').length;
              desiredTasks = desiredFromTasks || totalTasks;
            }
          }
          
          // Service is healthy if running count matches desired AND desired > 0
          const isHealthy = desiredTasks > 0 && runningTasks >= desiredTasks;

          let cpuUsage = 0;
          let memoryUsage = 0;

          // Omit per-service container stats to improve listing performance

          servicesWithStatusInner.push({
            id: serviceInfo.ID,
            name: serviceInfo.Spec.Name,
            status: isHealthy ? 'healthy' : 'unhealthy',
            running: runningTasks,
            desired: desiredTasks,
            total: totalTasks,
            image: serviceInfo.Spec.TaskTemplate.ContainerSpec?.Image || 'unknown',
            mode: serviceInfo.Spec.Mode.Replicated ? 'replicated' : 'global',
            cpuUsage: Math.round(cpuUsage * 1000) / 1000,
            memoryUsage: Math.round(memoryUsage * 1000) / 1000,
            tasks: []
          });
        }
        return servicesWithStatusInner;
      } catch (connectionError) {
        console.error(`Connection error to node ${nodeName}:`, connectionError.message);
        dropConnection(nodeName);
        return [];
      }
    });
    
    clearTimeout(abortTimeoutId);
    // Cache the result
    servicesCache.set(nodeName, { timestamp: Date.now(), data: servicesWithStatus });
    res.json(servicesWithStatus);
  } catch (error) {
    clearTimeout(abortTimeoutId);
    if (error.message === 'Request aborted') {
      console.error('Services request timed out for:', req.query.nodeName);
      dropConnection(req.query.nodeName);
      // Return cached data on timeout if available
      const cached = servicesCache.get(nodeName);
      if (cached) return res.json(cached.data);
      return res.status(504).json({ error: 'Request timeout' });
    }
    console.error('Error fetching services:', error);
    // Return cached data on error if available
    const cached = servicesCache.get(nodeName);
    if (cached) return res.json(cached.data);
    res.status(500).json({ error: error.message });
  }
});

// Get nodes information for a specific swarm node
app.get('/api/nodes', async (req, res) => {
  try {
    const { nodeName } = req.query;
    
    if (!nodeName) {
      return res.status(400).json({ error: 'nodeName parameter is required' });
    }
    const nodesInfo = await runWithNodeLock(nodeName, async () => {
      const docker = await getDockerConnection(nodeName);
      const nodes = await docker.listNodes();
      return nodes.map(node => ({
        hostname: node.Description?.Hostname || 'unknown',
        status: node.Status?.State || 'unknown',
        availability: node.Spec?.Availability || 'active',
        role: node.Spec?.Role || 'worker'
      }));
    });
    res.json(nodesInfo);
  } catch (error) {
    console.error('Error fetching nodes:', error);
    dropConnection(req.query?.nodeName);
    res.status(500).json({ error: error.message });
  }
});

// Helper function to get CPU usage - Alpine Linux uses different format
async function getCpuUsage() {
  try {
    // Try reading from /proc/stat first (more accurate), fall back to top if needed
    const { stdout: statOutput } = await execPromise(`grep "^cpu " /proc/stat | awk '{idle=$5; sum=$2+$3+$4+$5+$6+$7+$8+$9+$10; print 100*((sum-idle)/sum)}'`);
    let usage = parseFloat(statOutput.trim());
    
    // If that fails, use top
    if (isNaN(usage)) {
      const { stdout: topOutput } = await execPromise(`top -bn1 | grep "CPU:" | awk '{idle=$8; gsub("%","",idle); print 100-idle}'`);
      usage = parseFloat(topOutput.trim());
    }
    
    return isNaN(usage) ? 0 : Math.round(usage * 1000) / 1000;
  } catch (err) {
    console.error('Error reading CPU stats:', err);
    return 0;
  }
}

// Helper function to get memory usage - use /proc/meminfo for accuracy
async function getMemoryUsage() {
  try {
    const { stdout } = await execPromise(`awk '/^MemTotal:/ {total=$2} /^MemAvailable:/ {avail=$2} END {printf "%.3f", (total-avail)/total*100}' /proc/meminfo`);
    const usage = parseFloat(stdout.trim());
    return isNaN(usage) ? 0 : Math.round(usage * 1000) / 1000;
  } catch (err) {
    console.error('Error reading memory stats:', err);
    return 0;
  }
}

// Helper function to get disk usage
async function getDiskUsage() {
  try {
    const { stdout } = await execPromise("df / | tail -1 | awk '{print $5}' | sed 's/%//'");
    const usage = parseFloat(stdout.trim());
    return isNaN(usage) ? 0 : Math.round(usage * 1000) / 1000;
  } catch (err) {
    console.error('Error reading disk stats:', err);
    return 0;
  }
}

// Get system usage statistics for selected node
app.get('/api/usage', async (req, res) => {
  try {
    const { nodeName } = req.query;
    const defaultLocalNode = nodeConfigs.find(n => n.type === 'local')?.name || DEFAULT_LOCAL_NODE_NAME;
    const targetNode = nodeName || defaultLocalNode;
    const config = nodeConfigs.find(n => n.name === targetNode);
    if (!config) return res.status(400).json({ error: `Node ${targetNode} not found` });

    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');

    let cpuUsage = 0, memoryUsage = 0, diskUsage = 0;

    const docker = await runWithNodeLock(targetNode, async () => getDockerConnection(targetNode));

    if (config.type === 'local') {
      try {
        const { stdout: statOutput } = await execPromise(`grep "^cpu " /proc/stat | awk '{idle=$5; sum=$2+$3+$4+$5+$6+$7+$8+$9+$10; print 100*((sum-idle)/sum)}'`);
        cpuUsage = parseFloat(statOutput.trim()) || 0;
      } catch {}
      try {
        const { stdout } = await execPromise(`awk '/^MemTotal:/ {total=$2} /^MemAvailable:/ {avail=$2} END {printf "%.1f", (total-avail)/total*100}' /proc/meminfo`);
        memoryUsage = parseFloat(stdout.trim()) || 0;
      } catch {}
      try {
        const { stdout } = await execPromise("df / | tail -1 | awk '{print $5}' | sed 's/%//'");
        diskUsage = parseFloat(stdout.trim()) || 0;
      } catch {}
    } else {
      try {
        const cpuOut = await docker.execHost(`grep "^cpu " /proc/stat | awk '{idle=$5; sum=$2+$3+$4+$5+$6+$7+$8+$9+$10; print 100*((sum-idle)/sum)}'`);
        cpuUsage = parseFloat(String(cpuOut).trim()) || 0;
      } catch {}
      try {
        const memOut = await docker.execHost(`awk '/^MemTotal:/ {total=$2} /^MemAvailable:/ {avail=$2} END {printf "%.1f", (total-avail)/total*100}' /proc/meminfo`);
        memoryUsage = parseFloat(String(memOut).trim()) || 0;
      } catch {}
      try {
        const diskOut = await docker.execHost("df / | tail -1 | awk '{print $5}' | sed 's/%//'");
        diskUsage = parseFloat(String(diskOut).trim()) || 0;
      } catch {}
    }

    res.json({
      cpu: Math.round(cpuUsage * 1000) / 1000,
      memory: Math.round(memoryUsage * 1000) / 1000,
      disk: Math.round(diskUsage * 1000) / 1000
    });
  } catch (error) {
    console.error('Error fetching usage:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get per-service usage for a specific node (CPU/memory)
// Uses a cached approach to avoid timeout issues
const usageCache = new Map(); // { nodeName: { timestamp, data } }
const USAGE_CACHE_TTL = 2000; // 2 seconds - update every 2 seconds max
const nodeAbortControllers = new Map(); // Per-node abort controllers for clean cancellation
const activeFetches = new Map(); // Track ongoing fetches to prevent concurrent requests

app.get('/api/services-usage', async (req, res) => {
  const { nodeName } = req.query; // Extract at top level so it's available in all catch blocks
  
  try {
    if (!nodeName) return res.status(400).json({ error: 'nodeName parameter is required' });
    const config = nodeConfigs.find(n => n.name === nodeName);
    if (!config) return res.status(400).json({ error: `Node ${nodeName} not found` });

    // Check cache first - return cached data immediately
    const cached = usageCache.get(nodeName);
    const now = Date.now();
    const cacheAge = cached ? now - cached.timestamp : Infinity;
    
    // If cache is fresh, return it immediately
    if (cached && cacheAge < USAGE_CACHE_TTL) {
      return res.json(cached.data);
    }
    
    // If there's an active fetch, return old cache (prevents zeros)
    if (activeFetches.has(nodeName)) {
      if (cached) {
        return res.json(cached.data);
      }
      // Wait a bit for first fetch to complete
      await new Promise(r => setTimeout(r, 500));
      const newCache = usageCache.get(nodeName);
      return res.json(newCache ? newCache.data : []);
    }

    // Mark this node as having an active fetch
    activeFetches.set(nodeName, true);
    
    // Cancel any pending request for this node
    const oldController = nodeAbortControllers.get(nodeName);
    if (oldController) oldController.abort();
    
    // Create new abort controller for this request
    const nodeAbortController = new AbortController();
    nodeAbortControllers.set(nodeName, nodeAbortController);
    
    // Direct timeout (bypass runWithNodeLock to avoid timeout conflicts)
    const isSSH = config.type === 'ssh';
    const timeoutMs = isSSH ? 120000 : 15000; // 120s for SSH (many services can take time), 15s for local
    const timeoutId = setTimeout(() => {
      nodeAbortController.abort();
      console.log(`[${nodeName}] Request timeout after ${timeoutMs}ms`);
    }, timeoutMs);

    // Start background fetch without blocking response
    const usages = [];
    
    try {
      // Get docker connection FIRST, then check abort
      const docker = await getDockerConnection(nodeName);
      if (nodeAbortController.signal.aborted) {
        clearTimeout(timeoutId);
        return res.json(usageCache.get(nodeName)?.data || []);
      }
      
      const services = await docker.listServices();

      // Use batch docker stats for both local and SSH nodes
      // This gets ALL container stats in ONE command and aggregates by service
      console.log(`[${nodeName}] Fetching usage for ${services.length} services with batch stats...`);
      
      // Step 1: Get ALL container stats in ONE command
      // Container names in swarm follow pattern: servicename.replica.taskid
      let allStats = {};
      
      try {
        if (config.type === 'ssh') {
          // SSH node - use docker stats command via SSH
          const statsCmd = `docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemPerc}}" 2>/dev/null || echo ""`;
          const statsOutput = await Promise.race([
            docker.execHost(statsCmd),
            new Promise((_, reject) => setTimeout(() => reject(new Error('Stats timeout')), 30000))
          ]);
          
          // Parse stats output - each line is "containername,cpu%,mem%"
          const lines = statsOutput.trim().split('\n').filter(l => l.trim());
          console.log(`[${nodeName}] Got stats for ${lines.length} containers via SSH`);
          
          for (const line of lines) {
            const parts = line.split(',');
            if (parts.length >= 3) {
              const containerName = parts[0].trim();
              const cpuStr = parts[1].trim().replace(/[%\s]/g, '').replace('-', '0');
              const memStr = parts[2].trim().replace(/[%\s]/g, '').replace('-', '0');
              const cpuVal = parseFloat(cpuStr) || 0;
              const memVal = parseFloat(memStr) || 0;
              
              // Extract service name from container name (format: servicename.replica.taskid)
              const nameParts = containerName.split('.');
              if (nameParts.length >= 2) {
                const serviceName = nameParts[0];
                if (!allStats[serviceName]) {
                  allStats[serviceName] = { cpu: cpuVal, mem: memVal, count: 1 };
                } else {
                  allStats[serviceName].cpu += cpuVal;
                  allStats[serviceName].mem += memVal;
                  allStats[serviceName].count++;
                }
              }
            }
          }
        } else {
          // Local node - use Dockerode API to get container stats
          const containers = await docker.listContainers({ all: false }); // Only running containers
          console.log(`[${nodeName}] Got ${containers.length} running containers via Dockerode API`);
          
          // Fetch stats for all containers in parallel (with limit)
          const MAX_CONCURRENT = 10;
          for (let i = 0; i < containers.length; i += MAX_CONCURRENT) {
            const batch = containers.slice(i, i + MAX_CONCURRENT);
            const batchResults = await Promise.allSettled(batch.map(async (containerInfo) => {
              try {
                const container = docker.getContainer(containerInfo.Id);
                const statsData = await Promise.race([
                  container.stats({ stream: false }),
                  new Promise((_, reject) => setTimeout(() => reject(new Error('Stats timeout')), 5000))
                ]);
                
                // Calculate CPU percentage
                let cpuPercent = 0;
                if (statsData.cpu_stats && statsData.precpu_stats) {
                  const cpuDelta = (statsData.cpu_stats.cpu_usage?.total_usage || 0) - (statsData.precpu_stats.cpu_usage?.total_usage || 0);
                  const systemDelta = (statsData.cpu_stats.system_cpu_usage || 0) - (statsData.precpu_stats.system_cpu_usage || 0);
                  const numCpus = statsData.cpu_stats.online_cpus || statsData.cpu_stats.cpu_usage?.percpu_usage?.length || 1;
                  if (systemDelta > 0 && cpuDelta >= 0) {
                    cpuPercent = (cpuDelta / systemDelta) * numCpus * 100;
                  }
                }
                
                // Calculate Memory percentage
                let memPercent = 0;
                if (statsData.memory_stats && statsData.memory_stats.limit > 0) {
                  const memUsed = (statsData.memory_stats.usage || 0) - (statsData.memory_stats.stats?.cache || 0);
                  memPercent = (memUsed / statsData.memory_stats.limit) * 100;
                }
                
                // Get container name and extract service name
                // Container names are like /servicename.replica.taskid
                let containerName = containerInfo.Names?.[0] || '';
                if (containerName.startsWith('/')) containerName = containerName.substring(1);
                
                return { containerName, cpuPercent, memPercent };
              } catch (err) {
                return null;
              }
            }));
            
            for (const result of batchResults) {
              if (result.status === 'fulfilled' && result.value) {
                const { containerName, cpuPercent, memPercent } = result.value;
                const nameParts = containerName.split('.');
                if (nameParts.length >= 2) {
                  const serviceName = nameParts[0];
                  if (!allStats[serviceName]) {
                    allStats[serviceName] = { cpu: cpuPercent, mem: memPercent, count: 1 };
                  } else {
                    allStats[serviceName].cpu += cpuPercent;
                    allStats[serviceName].mem += memPercent;
                    allStats[serviceName].count++;
                  }
                }
              }
            }
          }
        }
        
        console.log(`[${nodeName}] Parsed stats for ${Object.keys(allStats).length} services`);
        
        // Debug: log some stats
        const statsKeys = Object.keys(allStats).slice(0, 5);
        for (const key of statsKeys) {
          const s = allStats[key];
          console.log(`[${nodeName}] Sample: ${key} CPU=${s.cpu.toFixed(2)}% MEM=${s.mem.toFixed(2)}%`);
        }
      } catch (statsErr) {
        console.error(`[${nodeName}] Batch stats error:`, statsErr.message);
      }
      
      // Step 2: Map services to stats
      for (const service of services) {
        const serviceInfo = service.inspect ? await service.inspect() : service;
        const serviceName = serviceInfo.Spec?.Name || 'unknown';
        const stats = allStats[serviceName];
        
        let cpuUsage = 0;
        let memoryUsage = 0;
        
        if (stats) {
          cpuUsage = stats.cpu;
          memoryUsage = stats.mem;
          if (cpuUsage > 0.01 || memoryUsage > 0.01) {
            console.log(`[${nodeName}] ${serviceName}: CPU=${cpuUsage.toFixed(2)}% MEM=${memoryUsage.toFixed(2)}% (${stats.count} replica${stats.count > 1 ? 's' : ''})`);
          }
        }
        
        usages.push({
          id: serviceInfo.ID,
          name: serviceName,
          cpuUsage: Math.min(100, Math.max(0, Math.round(cpuUsage * 1000) / 1000)),
          memoryUsage: Math.min(100, Math.max(0, Math.round(memoryUsage * 1000) / 1000))
        });
      }
      
      clearTimeout(timeoutId);
      activeFetches.delete(nodeName); // Clean up active fetch marker
      // Only cache if we have actual non-zero usage data
      const hasRealData = usages.some(u => u.cpuUsage > 0 || u.memoryUsage > 0);
      if (hasRealData || usages.length > 0) {
        usageCache.set(nodeName, { timestamp: Date.now(), data: usages });
      }
      res.json(usages);
    } catch (error) {
      clearTimeout(timeoutId);
      activeFetches.delete(nodeName); // Clean up active fetch marker on error
      console.error('Error fetching services usage:', error.message);
      // Return cached data on error to maintain stability
      // Return empty array ONLY if no cache exists (first request failure)
      const cached = usageCache.get(nodeName);
      if (cached && Date.now() - cached.timestamp < USAGE_CACHE_TTL * 2) {
        // Cache is still valid or only slightly expired - use it
        res.json(cached.data);
      } else if (cached) {
        // Cache is stale but better than nothing
        res.json(cached.data);
      } else {
        // No cache available - empty response is ok on first failure
        res.json([]);
      }
    }
  } catch (error) {
    if (nodeName) activeFetches.delete(nodeName); // Clean up active fetch marker on outer error
    console.error(`Error in services-usage endpoint for node ${nodeName}:`, error.message, error.stack);
    res.status(500).json({ error: error.message });
  }
});


// Get logs for a specific service on a specific node
app.get('/api/logs/:serviceId', async (req, res) => {
  try {
    const { serviceId } = req.params;
    const { count, nodeName } = req.query;
    
    console.log(`[/api/logs/${serviceId}] Request - nodeName: ${nodeName}, count: ${count}`);
    
    if (!nodeName) {
      return res.status(400).json({ error: 'nodeName parameter is required' });
    }
    
    // Validate log count - max 10,000 to keep browser rendering responsive.
    const requestedCount = parseInt(count) || 50;
    if (requestedCount > MAX_LOG_ROWS) {
      return res.status(400).json({ 
        error: `Log count exceeds maximum limit of ${MAX_LOG_ROWS}`,
        maxLimit: MAX_LOG_ROWS
      });
    }
    
    console.log(`[/api/logs/${serviceId}] Getting docker connection for node: ${nodeName}`);
    let docker = await runWithNodeLock(nodeName, async () => getDockerConnection(nodeName));
    console.log(`[/api/logs/${serviceId}] Docker connection obtained...`);

    // Get node config to check if it's SSH or local
    const config = nodeConfigs.find(n => n.name === nodeName);
    if (!config) {
      return res.status(400).json({ error: `Node ${nodeName} not found in configuration` });
    }

    // Logs are a swarm operation. If the selected node is a worker, delegate to a
    // manager of its swarm — a manager can stream the logs of the worker's tasks
    // cluster-wide. Managers resolve to themselves, so their flow is unchanged.
    if (config.type === 'ssh') {
      docker = await docker.getSwarmDocker();
    }

    const serviceInfo = await docker.getService(serviceId).inspect();
    console.log(`[/api/logs/${serviceId}] Service name: ${serviceInfo.Spec.Name}, getting tasks...`);

    // skipContainerLookup: the SSH primary path uses `docker service logs <id>`
    // and never needs per-task container IDs. Looking them up costs 1-2 extra
    // serialized SSH round-trips per task, which dominates latency on services
    // with many historical tasks. The rare fallback path resolves IDs lazily.
    const tasks = await docker.listTasks({
      filters: { service: [serviceId] },
      skipContainerLookup: true
    });
    console.log(`[/api/logs/${serviceId}] Tasks found: ${tasks.length}`);

    if (!tasks || tasks.length === 0) {
      console.log(`[/api/logs/${serviceId}] No tasks found, returning empty array`);
      return res.json([]);
    }

      console.log(`[/api/logs/${serviceId}] Preparing latest log fetch, count: ${requestedCount}`);

      // Keep fetch bounded for large requests so 10k retrieval stays responsive.
      const logFetchOverhead = Math.min(2000, Math.max(300, Math.floor(requestedCount * 0.1)));
      const maxRawLines = requestedCount + logFetchOverhead;

      const LOG_LEVEL_PATTERN = /\b(INFO|DEBUG|WARN|WARNING|ERROR|TRACE|FATAL|INF|ERR|WRN|DBG)\b/i;
      const STACKTRACE_CONTINUATION_PATTERN = /^(?:at\s+\S|\.\.\.\s+\d+\s+more$|Caused by:|Suppressed:|Wrapped by:)/;
      // NOTE: must stay linear-time. A nested quantifier like
      // /^(?:[a-zA-Z_$][\w$.]+)*(?:Exception|Error)(?::|$)/ causes catastrophic
      // backtracking (ReDoS) on long word-ish lines and pegs the event loop at
      // 100% CPU, which is what made large log fetches hang. The flat char class
      // below matches the same class-name shapes (e.g. java.lang.NullPointerException:)
      // without nesting.
      const EXCEPTION_LINE_PATTERN = /^[\w$.]*(?:Exception|Error)(?::|$)/;

      const normalizeLogLine = (line) => String(line || '')
        .replace(/\r/g, '')
        .replace(/\u001b\[[0-9;]*m/g, '')
        .replace(/^[\u0000-\u001f]+/, '');

      const decodeDockerLogPayload = (value) => {
        if (!Buffer.isBuffer(value)) return String(value || '');

        // Docker non-TTY logs may use an 8-byte multiplexed frame header.
        // Decode framed payload safely; if format doesn't match, fall back to plain text.
        const chunks = [];
        let offset = 0;
        let framed = false;

        while (offset + 8 <= value.length) {
          const streamType = value[offset];
          const payloadLen = value.readUInt32BE(offset + 4);
          const frameEnd = offset + 8 + payloadLen;

          if ((streamType !== 1 && streamType !== 2) || frameEnd > value.length) {
            framed = false;
            break;
          }

          framed = true;
          chunks.push(value.slice(offset + 8, frameEnd));
          offset = frameEnd;
        }

        if (framed && offset === value.length) {
          return Buffer.concat(chunks).toString('utf8');
        }

        return value.toString('utf8');
      };

      const extractSessionId = (value) => {
        const sessionMatch = String(value || '').match(/\[([a-zA-Z0-9_-]+(?:::|:)\d+)\]/);
        let sessionId = sessionMatch ? sessionMatch[1] : null;
        if (sessionId && !sessionId.includes('::') && sessionId.length <= 15) {
          sessionId = null;
        }
        return sessionId;
      };

      const cleanLogMessage = (value) => {
        let cleanedMessage = String(value || '');

        cleanedMessage = cleanedMessage.replace(/^.*?\b(INFO|DEBUG|WARN|WARNING|ERROR|TRACE|FATAL|INF|ERR|WRN|DBG)\b\s*/, '');
        cleanedMessage = cleanedMessage.replace(/^\d{4}-\d{2}-\d{2}T[\d:.+-]*Z?\s*/, '');

        let prevMessage = '';
        while (cleanedMessage !== prevMessage && /^\[[^\]]+\]\s*/.test(cleanedMessage)) {
          prevMessage = cleanedMessage;
          cleanedMessage = cleanedMessage.replace(/^\[[^\]]+\]\s*/, '');
        }

        const dashIndex = cleanedMessage.indexOf(' - ');
        if (dashIndex > 0) {
          const afterDash = cleanedMessage.substring(dashIndex + 3);
          if (/^(OK|finished|completed|SUCCESS|FAILED)/i.test(afterDash)) {
            cleanedMessage = cleanedMessage.substring(0, dashIndex);
          }
        }

        return cleanedMessage.trimEnd();
      };

      const detectLogLevel = (value) => {
        const messageLower = String(value || '').toLowerCase();
        if (messageLower.includes('error') || messageLower.includes('fatal') || messageLower.includes('exception')) return 'error';
        if (messageLower.includes('warn')) return 'warning';
        if (messageLower.includes('debug')) return 'debug';
        return 'info';
      };

      const buildParsedEntry = ({ timestamp, rawMessage, fallbackFlowId }) => {
        const message = cleanLogMessage(rawMessage);
        const explicitLevel = LOG_LEVEL_PATTERN.test(String(rawMessage || ''));
        const continuationHint = STACKTRACE_CONTINUATION_PATTERN.test(message)
          || /^\s+at\s+\S/.test(String(rawMessage || ''))
          || EXCEPTION_LINE_PATTERN.test(message);

        return {
          timestamp,
          message,
          level: detectLogLevel(rawMessage),
          flowId: extractSessionId(rawMessage) || fallbackFlowId,
          explicitLevel,
          continuationHint
        };
      };

      const coalesceMultilineEntries = (entries) => {
        const grouped = [];
        let current = null;

        for (const entry of entries) {
          if (!entry || !entry.message) continue;

          const shouldAppend = Boolean(
            current
            && (
              entry.continuationHint
              || (!entry.explicitLevel && current.level === 'error')
            )
          );

          if (shouldAppend) {
            current.message = `${current.message}\n${entry.message}`;
            continue;
          }

          if (current) {
            grouped.push(current);
          }

          current = { ...entry };
        }

        if (current) {
          grouped.push(current);
        }

        return grouped.map(({ explicitLevel, continuationHint, ...entry }) => entry);
      };

      const parseServiceLogLines = (lines, idPrefix) => {
        const serviceRegex = /(\d{4}-\d{2}-\d{2}T[\d:.]+Z?)\s+[^|]+\|\s*(.*)$/;
        return lines.map((line, idx) => {
          const normalizedLine = normalizeLogLine(line);
          const match = normalizedLine.match(serviceRegex);
          if (match) {
            const [, ts, rawMessage] = match;
            return buildParsedEntry({
              timestamp: ts.endsWith('Z') ? ts : `${ts}Z`,
              rawMessage,
              fallbackFlowId: `${idPrefix}-${idx}`
            });
          }

          return buildParsedEntry({
            timestamp: new Date().toISOString(),
            rawMessage: normalizedLine,
            fallbackFlowId: `${idPrefix}-raw-${idx}`
          });
        }).filter(Boolean);
      };

      // Parser helper
      const parseLogs = (raw, namePrefix, maxLines = maxRawLines) => {
        const text = decodeDockerLogPayload(raw);
        const allLines = text.split(/\r?\n/).filter(l => l.trim());
        const lines = allLines.length > maxLines ? allLines.slice(-maxLines) : allLines;
        const regexTs = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)?\s*(.+)$/;
        const parsedLines = lines.map((line, index) => {
          const normalizedLine = normalizeLogLine(line);
          const m = normalizedLine.match(regexTs);
          let iso = new Date().toISOString();
          let msg = m ? m[2] : normalizedLine;
          if (m && m[1]) {
            iso = m[1].endsWith('Z') ? m[1] : m[1] + 'Z';
          }

          return buildParsedEntry({
            timestamp: iso,
            rawMessage: msg,
            fallbackFlowId: `${namePrefix}-${index}`
          });
        });

        return coalesceMultilineEntries(parsedLines);
      };

      console.log(`[/api/logs/${serviceId}] Fetching latest logs, node type: ${config.type}`);

        // For SSH manager nodes, always use 'docker service logs' which works across all swarm nodes
        // and includes logs from all tasks (running, failed, shutdown)
        if (config.type === 'ssh') {
          console.log(`[/api/logs/${serviceId}] SSH node - using docker service logs for service ID: ${serviceId}`);
          try {
            const cmd = `docker service logs ${serviceId} --timestamps --tail ${maxRawLines} 2>&1`;
            console.log(`[/api/logs/${serviceId}] Executing: ${cmd}`);
            let raw = '';
            try {
              // Large fetches (up to 10k lines) need more than the default 10s
              // per-command budget for docker to gather + stream the output.
              const logFetchTimeout = Math.min(60000, 15000 + requestedCount * 3);
              raw = await docker.execHost(cmd, logFetchTimeout);
            } catch (logErr) {
              console.log(`[/api/logs/${serviceId}] Primary SSH service log fetch failed, trying fallback: ${logErr.message}`);
            }
            console.log(`[/api/logs/${serviceId}] Raw output length: ${raw.length} bytes`);

            let lines = raw.trim().split(/\r?\n/).filter(l => l.trim());
            if (lines.length > maxRawLines) {
              lines = lines.slice(-maxRawLines);
            }

            if (lines.length === 0) {
              console.log(`[/api/logs/${serviceId}] SSH service logs empty, falling back to recent task logs`);
              const taskCandidates = tasks
                .slice()
                .sort((a, b) => Date.parse(b.UpdatedAt || 0) - Date.parse(a.UpdatedAt || 0))
                .slice(0, Math.min(tasks.length, 8));
              const fallbackLines = [];

              for (const task of taskCandidates) {
                const taskShortId = (task.ID || '').slice(0, 12);
                let taskRaw = '';

                try {
                  const containerId = task.Status?.ContainerStatus?.ContainerID;
                  if (containerId) {
                    taskRaw = await docker.getContainer(containerId).logs({
                      stdout: true,
                      stderr: true,
                      timestamps: true,
                      tail: Math.max(requestedCount * 4, 50)
                    });
                  }
                } catch (taskErr) {
                  console.log(`[/api/logs/${serviceId}] SSH container log fallback failed for task ${taskShortId}: ${taskErr.message}`);
                }

                if (!String(taskRaw || '').trim() && taskShortId) {
                  try {
                    taskRaw = await docker.execHost(`docker service logs ${serviceId} --timestamps --tail ${Math.max(maxRawLines, 300)} 2>&1 | grep -F '${taskShortId}' || true`);
                  } catch (taskErr) {
                    console.log(`[/api/logs/${serviceId}] SSH task-stream fallback failed for task ${taskShortId}: ${taskErr.message}`);
                  }
                }

                const taskLines = String(taskRaw || '').split(/\r?\n/).filter(l => l.trim());
                if (taskLines.length) {
                  fallbackLines.push(...taskLines);
                }
              }

              lines = fallbackLines;
              console.log(`[/api/logs/${serviceId}] SSH fallback produced ${lines.length} lines`);
            }

            console.log(`[/api/logs/${serviceId}] Split into ${lines.length} lines`);
            
            const parsed = coalesceMultilineEntries(parseServiceLogLines(lines, 'task'));

            if (parsed.length === 0) {
              const synthesized = tasks
                .slice()
                .sort((a, b) => Date.parse(b.UpdatedAt || 0) - Date.parse(a.UpdatedAt || 0))
                .slice(0, requestedCount)
                .map((task, idx) => {
                  const state = task.Status?.State || 'unknown';
                  const err = task.Status?.Err || '';
                  const statusMessage = task.Status?.Message || '';
                  const message = err
                    ? `Task ${task.ID.slice(0, 12)} ${state}: ${err}`
                    : statusMessage
                      ? `Task ${task.ID.slice(0, 12)} ${state}: ${statusMessage}`
                      : `Task ${task.ID.slice(0, 12)} ${state}: no retained container logs were available on the remote node`;
                  const level = state === 'failed' || state === 'rejected'
                    ? 'error'
                    : state === 'shutdown'
                      ? 'warning'
                      : 'info';
                  return {
                    timestamp: task.UpdatedAt || new Date().toISOString(),
                    message,
                    level,
                    flowId: `task-status-${idx}`
                  };
                });

              console.log(`[/api/logs/${serviceId}] SSH synthesized ${synthesized.length} task-status entries`);
              return res.json(synthesized);
            }
            
            console.log(`[/api/logs/${serviceId}] Parsed ${parsed.length} log entries, returning last ${requestedCount}`);
            return res.json(parsed.slice(-requestedCount));
          } catch (err) {
            console.error(`[/api/logs/${serviceId}] Error fetching service logs:`, err.message);
            console.error(`[/api/logs/${serviceId}] Error stack:`, err.stack);
            return res.json([]);
          }
        }
        
        // For local node, aggregate logs from the newest task containers.
        // This covers both running tasks and fast-failing crash-loop tasks.
        console.log(`[/api/logs/${serviceId}] Local node - aggregating latest logs from newest task containers`);
        const taskCandidates = tasks
          .filter(t => t.Status.ContainerStatus?.ContainerID)
          .sort((a, b) => Date.parse(b.UpdatedAt || 0) - Date.parse(a.UpdatedAt || 0));

        if (!taskCandidates.length) {
          console.log(`[/api/logs/${serviceId}] No task containers found for local node`);
          return res.json([]);
        }

        const maxTasksToQuery = Math.min(taskCandidates.length, 12);
        const selectedTasks = taskCandidates.slice(0, maxTasksToQuery);
        const perTaskTail = Math.max(100, Math.ceil((requestedCount * 6) / selectedTasks.length));
        const collected = [];

        for (const task of selectedTasks) {
          try {
            const containerId = task.Status.ContainerStatus.ContainerID;
            const c = docker.getContainer(containerId);
            const raw = await c.logs({
              stdout: true,
              stderr: true,
              timestamps: true,
              tail: perTaskTail
            });
            const parsed = parseLogs(raw, `${serviceInfo.Spec.Name}-task${task.Slot || 0}`);
            collected.push(...parsed);
          } catch (err) {
            console.error(`[/api/logs/${serviceId}] Error fetching local container logs for task ${task.ID.substring(0, 12)}:`, err.message);
          }
        }

        if (!collected.length) {
          console.log(`[/api/logs/${serviceId}] No logs available from local task containers`);
          return res.json([]);
        }

        collected.sort((a, b) => Date.parse(a.timestamp) - Date.parse(b.timestamp));
        console.log(`[/api/logs/${serviceId}] Local aggregation returned ${collected.length} entries, returning last ${requestedCount}`);
        return res.json(collected.slice(-requestedCount));
  } catch (error) {
    console.error(`[/api/logs/${req.params.serviceId}] Error for node ${req.query.nodeName}:`, error.message);
    res.status(500).json({ error: error.message });
  }
});

// Service control endpoints (start/stop/restart)
app.post('/api/service/:serviceId/scale', async (req, res) => {
  try {
    const { serviceId } = req.params;
    const { replicas, nodeName } = req.body;
    
    if (!nodeName) {
      return res.status(400).json({ error: 'nodeName is required' });
    }
    
    if (typeof replicas !== 'number' || replicas < 0) {
      return res.status(400).json({ error: 'replicas must be a non-negative number' });
    }
    
    console.log(`[Service Control] Scaling ${serviceId} to ${replicas} replicas on ${nodeName}`);
    
    const config = nodeConfigs.find(n => n.name === nodeName);
    if (!config) {
      return res.status(400).json({ error: `Node ${nodeName} not found` });
    }
    
    await runWithNodeLock(nodeName, async () => {
      let docker = await getDockerConnection(nodeName);

      if (config.type === 'ssh') {
        // Scaling is a swarm operation — run it on a manager of this swarm.
        // For a worker node this delegates to its manager; managers use themselves.
        docker = await docker.getSwarmDocker();
        await docker.execHost(`docker service scale ${serviceId}=${replicas}`);
      } else {
        // Use dockerode API for local node
        const service = docker.getService(serviceId);
        const serviceInfo = await service.inspect();
        
        // Update the service spec with new replica count
        const updateSpec = {
          ...serviceInfo.Spec,
          Mode: {
            Replicated: {
              Replicas: replicas
            }
          }
        };
        
        await service.update({
          version: serviceInfo.Version.Index,
          ...updateSpec
        });
      }
    });
    
    // Clear messages cache to reflect the change immediately
    messagesCache = { data: [], timestamp: 0 };
    
    console.log(`[Service Control] Successfully scaled ${serviceId} to ${replicas} replicas`);
    res.json({ success: true, message: `Service scaled to ${replicas} replicas` });
  } catch (error) {
    console.error('Error scaling service:', error);
    res.status(500).json({ error: error.message });
  }
});

app.post('/api/service/:serviceId/restart', async (req, res) => {
  try {
    const { serviceId } = req.params;
    const { nodeName } = req.body;
    
    if (!nodeName) {
      return res.status(400).json({ error: 'nodeName is required' });
    }
    
    console.log(`[Service Control] Restarting ${serviceId} on ${nodeName}`);
    
    const config = nodeConfigs.find(n => n.name === nodeName);
    if (!config) {
      return res.status(400).json({ error: `Node ${nodeName} not found` });
    }
    
    await runWithNodeLock(nodeName, async () => {
      let docker = await getDockerConnection(nodeName);

      if (config.type === 'ssh') {
        // Restart is a swarm operation — run it on a manager of this swarm.
        // For a worker node this delegates to its manager; managers use themselves.
        docker = await docker.getSwarmDocker();
        await docker.execHost(`docker service update --force ${serviceId}`);
      } else {
        // Use dockerode API for local node
        const service = docker.getService(serviceId);
        const serviceInfo = await service.inspect();
        
        // Force update to restart the service
        await service.update({
          version: serviceInfo.Version.Index,
          ...serviceInfo.Spec,
          TaskTemplate: {
            ...serviceInfo.Spec.TaskTemplate,
            ForceUpdate: (serviceInfo.Spec.TaskTemplate.ForceUpdate || 0) + 1
          }
        });
      }
    });
    
    // Clear messages cache to reflect the change
    messagesCache = { data: [], timestamp: 0 };
    
    console.log(`[Service Control] Successfully restarted ${serviceId}`);
    res.json({ success: true, message: 'Service restarted' });
  } catch (error) {
    console.error('Error restarting service:', error);
    res.status(500).json({ error: error.message });
  }
});

// Get service info (for getting current replica count)
app.get('/api/service/:serviceId/info', async (req, res) => {
  try {
    const { serviceId } = req.params;
    const { nodeName } = req.query;
    
    if (!nodeName) {
      return res.status(400).json({ error: 'nodeName is required' });
    }
    
    const config = nodeConfigs.find(n => n.name === nodeName);
    if (!config) {
      return res.status(400).json({ error: `Node ${nodeName} not found` });
    }
    
    const serviceInfo = await runWithNodeLock(nodeName, async () => {
      const docker = await getDockerConnection(nodeName);
      
      // getService().inspect() is swarm-role aware for SSH nodes (manager uses
      // `docker service inspect`; worker synthesizes from local containers).
      const service = docker.getService(serviceId);
      return await service.inspect();
    });
    
    res.json({
      id: serviceInfo.ID,
      name: serviceInfo.Spec?.Name,
      replicas: serviceInfo.Spec?.Mode?.Replicated?.Replicas ?? 0
    });
  } catch (error) {
    console.error('Error getting service info:', error);
    res.status(500).json({ error: error.message });
  }
});

// AI diagnosis removed in this build
// Serve static files
app.use(express.static(__dirname));

// Serve index.html as default
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

// Initialize and start server
const PORT = process.env.PORT || 3001;

// Global error handlers to prevent crashes
process.on('uncaughtException', (err) => {
  console.error('Uncaught Exception:', err);
  // Don't exit - log and continue
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
  // Don't exit - log and continue
});

loadNodeConfigs().then(() => {
  app.listen(PORT, '0.0.0.0', () => {
    console.log(`Docker Swarm Monitor running on http://0.0.0.0:${PORT}`);
    console.log(`Monitoring ${nodeConfigs.length} node(s)`);
    console.log(`Circuit breaker: ${MAX_FAILURES} failures, ${CIRCUIT_RESET_TIME/1000}s reset`);
    
    // Pre-warm the messages cache so first request is instant
    setTimeout(() => {
      console.log('Pre-warming messages cache...');
      refreshMessagesInBackground();
    }, 2000);
  });
}).catch(err => {
  console.error('Failed to load node configurations:', err);
  process.exit(1);
});
