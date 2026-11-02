// backend/server2.js
// Main API Server and Distributed Database Coordinator for GameVault

const express = require('express');
const cors = require('cors');
const cron = require('node-cron');

const {
  nodeConfigs,
  getActiveNodeKey,
  setActiveNodeKey,
  getNodeConfig,
  isSimulatedDown,
  setSimulatedFailure,
  pingNode,
  closeAllPools
} = require('./db');

const {
  getAllPendingReplications,
  getPendingReplicationsForNode
} = require('./replicationLog');

const coordinator = require('./coordinator');

const app = express();
const port = process.env.PORT || 5000;

// Middleware setup
app.use(cors());
app.use(express.json());

// Middleware to enforce read-only state when Node 1 is down
function checkNode1Down(req, res, next) {
  if (isSimulatedDown('node1') && ['POST', 'PUT', 'DELETE'].includes(req.method)) {
    return res.status(503).json({ error: 'Node1 is down, system is read-only' });
  }
  next();
}

// ----------------------- Management & Status Routes -----------------------

// Route to get current active node
app.get('/active-node', (req, res) => {
  const currentKey = getActiveNodeKey();
  res.json({
    node: currentKey,
    activeNode: getNodeConfig(currentKey),
    isDown: isSimulatedDown(currentKey)
  });
});

// Route to switch active node
app.post('/switch-node', (req, res) => {
  const { node } = req.body;
  if (!node || !nodeConfigs[node]) {
    return res.status(400).json({ error: 'Invalid node selection' });
  }

  if (isSimulatedDown(node)) {
    console.log(`[Server] Node ${node} is currently down, selecting alternate available node.`);
    const candidates = ['node1', 'node2', 'node3'].filter(n => n !== node && !isSimulatedDown(n));
    if (candidates.length === 0) {
      return res.status(503).json({ error: 'No available node to switch to.' });
    }
    setActiveNodeKey(candidates[0]);
    return res.json({ message: `Node ${node} is down, switched to ${candidates[0]}`, node: candidates[0] });
  }

  setActiveNodeKey(node);
  console.log(`[Server] Switched active node to ${node}`);
  res.json({ message: 'Node switched successfully', node });
});

// Route to simulate a node failure
app.post('/simulate-failure', (req, res) => {
  const { node } = req.body;
  if (!node || !nodeConfigs[node]) {
    return res.status(400).json({ error: 'Invalid node selection' });
  }

  setSimulatedFailure(node, true);
  console.log(`[Server] Node ${node} marked as failed.`);
  res.json({ message: `${node} marked as failed` });
});

// Route to simulate node recovery and trigger immediate recovery replay
app.post('/simulate-recovery', async (req, res) => {
  const { node } = req.body;
  if (!node || !nodeConfigs[node]) {
    return res.status(400).json({ error: 'Invalid node selection' });
  }

  try {
    const recoveryResult = await coordinator.recoverNodeAndReplay(node);
    if (recoveryResult.inProgress) {
      return res.status(200).json(recoveryResult);
    }
    res.json({
      message: `${node} recovered successfully`,
      replayedCount: recoveryResult.replayedCount,
      remainingPending: recoveryResult.remainingPending,
      inProgress: false
    });
  } catch (err) {
    console.error(`[Server] Error during recovery of ${node}:`, err);
    res.status(500).json({ error: `Recovery failed for ${node}: ${err.message}` });
  }
});

// Route to inspect failing nodes
app.get('/redirect-node', (req, res) => {
  const failingNodes = ['node1', 'node2', 'node3'].filter(n => isSimulatedDown(n));
  if (failingNodes.length > 0) {
    return res.status(200).json({
      status: 'error',
      message: `Node failure occurred in the following node(s): ${failingNodes.join(', ')}`,
      failingNodes
    });
  }
  res.status(200).json({ status: 'success', message: 'All nodes are operational' });
});

// Route to check Node 1 status for frontend CRUD enablement
app.get('/check-node1', (req, res) => {
  const isNode1Down = isSimulatedDown('node1');
  const isNode1Active = getActiveNodeKey() === 'node1';

  if (isNode1Down || !isNode1Active) {
    return res.status(200).json({
      status: 'error',
      message: 'CRUD Operations are unavailable due to Down Central Node'
    });
  }

  res.status(200).json({
    status: 'success',
    message: 'Node1 is operational'
  });
});

// Route to view pending replication queue status
app.get('/replication-queue', (req, res) => {
  res.json({
    pending: getAllPendingReplications()
  });
});

// ----------------------- Query Execution -----------------------

app.post('/query', async (req, res) => {
  const { query } = req.body;
  if (!query) {
    return res.status(400).json({ error: 'Query parameter is required' });
  }

  try {
    const result = await coordinator.executeQuery(query);
    res.json(result);
  } catch (err) {
    console.error('[Server] Query execution error:', err.message);
    const status = err.status || 500;
    res.status(status).json({ error: err.message });
  }
});

// Also support GET /query for flexible testing / clients
app.get('/query', async (req, res) => {
  const query = req.query.query || req.body?.query;
  if (!query) {
    return res.status(400).json({ error: 'Query parameter is required' });
  }

  try {
    const result = await coordinator.executeQuery(query);
    res.json(result);
  } catch (err) {
    const status = err.status || 500;
    res.status(status).json({ error: err.message });
  }
});

// ----------------------- Distributed CRUD Routes -----------------------

// Add record
app.post('/add-record', checkNode1Down, async (req, res) => {
  try {
    const result = await coordinator.addRecord(req.body);
    res.status(200).json({
      message: 'Record added successfully.',
      ...result
    });
  } catch (err) {
    console.error('[Server] Error adding record:', err.message);
    const status = err.status || 500;
    res.status(status).json({ error: err.message });
  }
});

// Update record
app.put('/update-record', checkNode1Down, async (req, res) => {
  try {
    const result = await coordinator.updateRecord(req.body);
    res.status(200).json({
      message: 'Record updated successfully',
      ...result
    });
  } catch (err) {
    console.error('[Server] Error updating record:', err.message);
    const status = err.status || 500;
    res.status(status).json({ error: err.message });
  }
});

// Delete record (supports both DELETE /delete-record and DELETE /delete-record/:id)
app.delete(['/delete-record', '/delete-record/:id'], checkNode1Down, async (req, res) => {
  const id = req.body?.id || req.params?.id || req.query?.id;
  if (!id) {
    return res.status(400).json({ error: 'Invalid ID' });
  }

  try {
    const result = await coordinator.deleteRecord(id);
    res.status(200).json({
      message: 'Data deleted successfully',
      ...result
    });
  } catch (err) {
    console.error('[Server] Error deleting record:', err.message);
    const status = err.status || 500;
    res.status(status).json({ error: err.message });
  }
});

// ----------------------- Periodic Background Health & Recovery -----------------------

let cronTask = null;
if (process.env.NODE_ENV !== 'test') {
  // Periodically check node health and trigger recovery replay
  cronTask = cron.schedule('*/30 * * * * *', async () => {
    for (const nodeKey of ['node1', 'node2', 'node3']) {
      const isDown = isSimulatedDown(nodeKey);
      if (isDown) {
        // Test connectivity if simulated flag is not manually locked
        continue;
      }

      // Check if there are pending replications waiting for this node
      const pending = getPendingReplicationsForNode(nodeKey);
      if (pending.length > 0) {
        console.log(`[Cron] Found ${pending.length} pending transactions for healthy ${nodeKey}. Replaying...`);
        try {
          await coordinator.recoverNodeAndReplay(nodeKey);
        } catch (err) {
          console.error(`[Cron] Recovery error for ${nodeKey}:`, err.message);
        }
      }
    }
  });
}

// Start server when run directly
let serverInstance = null;
if (process.env.NODE_ENV !== 'test') {
  serverInstance = app.listen(port, () => {
    console.log(`GameVault Distributed Database Coordinator running on port ${port}`);
  });
}

// Graceful cleanup function for tests or shutdown
async function closeServer() {
  if (cronTask) {
    cronTask.stop();
  }
  if (serverInstance) {
    await new Promise((resolve) => serverInstance.close(resolve));
  }
  await closeAllPools();
}

app.close = closeServer;

module.exports = app;
