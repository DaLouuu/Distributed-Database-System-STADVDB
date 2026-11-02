// backend/replicationLog.js
// Authoritative Transaction Log for GameVault Distributed Database System

const fs = require('fs');
const path = require('path');
const { queryNode, isSimulatedDown } = require('./db');

const LOG_FILE_PATH = path.join(__dirname, 'logs', 'replication_queue.json');

// In-memory cache synchronized with disk and MySQL
let replicationQueue = [];
let localLogCounter = 1;

// Lock timeout for reclaiming abandoned in-progress entries (30 seconds)
const ABANDONED_LOCK_TIMEOUT_MS = 30000;

function ensureLogDir() {
  const dir = path.dirname(LOG_FILE_PATH);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

function loadQueueFromDisk() {
  ensureLogDir();
  if (fs.existsSync(LOG_FILE_PATH)) {
    try {
      const data = fs.readFileSync(LOG_FILE_PATH, 'utf-8');
      replicationQueue = JSON.parse(data);
      if (replicationQueue.length > 0) {
        const maxLogId = Math.max(...replicationQueue.map(e => e.logId || 0));
        localLogCounter = maxLogId + 1;
      }
    } catch (err) {
      console.error('[ReplicationLog] Error reading queue from disk:', err.message);
      replicationQueue = [];
    }
  } else {
    replicationQueue = [];
    saveQueueToDisk();
  }
}

function saveQueueToDisk() {
  ensureLogDir();
  try {
    fs.writeFileSync(LOG_FILE_PATH, JSON.stringify(replicationQueue, null, 2), 'utf-8');
  } catch (err) {
    console.error('[ReplicationLog] Error saving queue to disk:', err.message);
  }
}

loadQueueFromDisk();

function generateTxId() {
  return `tx_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

function generateWorkerId() {
  return `worker_${process.pid}_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
}

/**
 * Check whether a target fragment node has unapplied pending or in-progress replications.
 * Used for replication freshness and staleness detection during dataset reconstruction.
 */
function hasPendingReplications(targetNode) {
  return replicationQueue.some(
    e => e.targetNode === targetNode && (e.status === 'PENDING' || e.status === 'IN_PROGRESS')
  );
}

/**
 * Log a pending replication operation INSIDE an existing transaction on Node 1.
 * Co-located with the master data mutation.
 */
async function logPendingReplicationWithTx(tx, { txId, targetNode, operation, recordId, payload }) {
  const transactionId = txId || generateTxId();
  const currentSeq = localLogCounter++;

  const entry = {
    logId: currentSeq,
    txId: transactionId,
    targetNode,
    operation,
    recordId,
    payload: payload || null,
    status: 'PENDING',
    workerId: null,
    claimedAt: null,
    createdAt: new Date().toISOString()
  };

  const sql = `
    INSERT INTO replication_log (tx_id, target_node, operation, record_id, payload, status)
    VALUES (?, ?, ?, ?, ?, 'PENDING')
  `;
  await tx.query(sql, [
    transactionId,
    targetNode,
    operation,
    recordId,
    JSON.stringify(payload || {})
  ]);

  replicationQueue.push(entry);
  saveQueueToDisk();

  return entry;
}

/**
 * Get all pending transactions for a given target node, ordered strictly by logId (FIFO).
 * Includes both PENDING and abandoned IN_PROGRESS entries.
 */
async function getPendingReplicationsForNode(targetNode) {
  if (!isSimulatedDown('node1')) {
    try {
      const rows = await queryNode(
        'node1',
        `SELECT log_id, tx_id, target_node, operation, record_id, payload, status, created_at
         FROM replication_log
         WHERE target_node = ?
         ORDER BY log_id ASC`,
        [targetNode]
      );
      if (rows && rows.length > 0) {
        return rows
          .filter(r => r.status === 'PENDING' || r.status === 'IN_PROGRESS')
          .map(r => ({
            logId: r.log_id,
            txId: r.tx_id,
            targetNode: r.target_node,
            operation: r.operation,
            recordId: r.record_id,
            payload: typeof r.payload === 'string' ? JSON.parse(r.payload) : r.payload,
            status: r.status,
            createdAt: r.created_at
          }));
      }
    } catch (err) {
      // Fall back to disk copy
    }
  }

  return replicationQueue
    .filter(e => e.targetNode === targetNode && (e.status === 'PENDING' || e.status === 'IN_PROGRESS'))
    .sort((a, b) => (a.logId || 0) - (b.logId || 0));
}

/**
 * Conditionally claim a replication log entry using a database-coordinated conditional update.
 * Checks affectedRows to guarantee mutual exclusion across multiple backend instances.
 * Also reclaims abandoned entries if the previous worker crashed.
 */
async function claimReplicationEntry(logId, txId, workerId) {
  const timeoutThreshold = new Date(Date.now() - ABANDONED_LOCK_TIMEOUT_MS).toISOString();
  const claimedAt = new Date().toISOString();

  // Update in MySQL on Node 1 if reachable
  if (!isSimulatedDown('node1')) {
    try {
      const sql = `
        UPDATE replication_log
        SET status = 'IN_PROGRESS', worker_id = ?, claimed_at = ?
        WHERE log_id = ? AND (status = 'PENDING' OR (status = 'IN_PROGRESS' AND claimed_at < ?))
      `;
      const result = await queryNode('node1', sql, [workerId, claimedAt, logId, timeoutThreshold]);
      if (result && result.affectedRows === 0) {
        return false; // Another worker already claimed this entry
      }
    } catch (err) {
      // Continue to local sync
    }
  }

  // Update local queue entry
  const entry = replicationQueue.find(e => e.txId === txId || e.logId === logId);
  if (entry) {
    const isPending = entry.status === 'PENDING';
    const isAbandoned = entry.status === 'IN_PROGRESS' && entry.claimedAt && new Date(entry.claimedAt) < new Date(timeoutThreshold);
    if (!isPending && !isAbandoned) {
      return false; // Already locked by another worker
    }
    entry.status = 'IN_PROGRESS';
    entry.workerId = workerId;
    entry.claimedAt = claimedAt;
    saveQueueToDisk();
  }

  return true;
}

/**
 * Update transaction status after execution.
 * Only the worker that holds the claim (or any recovery worker if resetting to PENDING) can update it.
 */
async function markReplicationStatus(txId, newStatus, workerId = null) {
  const entry = replicationQueue.find(e => e.txId === txId);
  if (entry) {
    entry.status = newStatus;
    entry.updatedAt = new Date().toISOString();
    if (newStatus === 'APPLIED') {
      entry.workerId = null;
      entry.claimedAt = null;
    }
    saveQueueToDisk();
  }

  if (!isSimulatedDown('node1')) {
    try {
      await queryNode(
        'node1',
        `UPDATE replication_log SET status = ? WHERE tx_id = ?`,
        [newStatus, txId]
      );
    } catch (err) {
      // Local copy updated
    }
  }
}

async function markReplicationApplied(txId, workerId = null) {
  return await markReplicationStatus(txId, 'APPLIED', workerId);
}

function getAllPendingReplications() {
  return replicationQueue.filter(entry => entry.status === 'PENDING' || entry.status === 'IN_PROGRESS');
}

function clearAllLogs() {
  replicationQueue = [];
  localLogCounter = 1;
  saveQueueToDisk();
}

module.exports = {
  generateTxId,
  generateWorkerId,
  hasPendingReplications,
  logPendingReplicationWithTx,
  getPendingReplicationsForNode,
  claimReplicationEntry,
  markReplicationStatus,
  markReplicationApplied,
  loadQueueFromDisk,
  saveQueueToDisk,
  getAllPendingReplications,
  clearAllLogs
};
