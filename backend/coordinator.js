// backend/coordinator.js
// Distributed Transaction Coordinator for GameVault Distributed Database System

const {
  queryNode,
  withTransaction,
  getActiveNodeKey,
  setActiveNodeKey,
  isSimulatedDown,
  setSimulatedFailure,
  pingNode,
  nodeConfigs
} = require('./db');

const {
  generateTxId,
  generateWorkerId,
  hasPendingReplications,
  logPendingReplicationWithTx,
  getPendingReplicationsForNode,
  claimReplicationEntry,
  markReplicationStatus,
  markReplicationApplied,
  getAllPendingReplications
} = require('./replicationLog');

/**
 * Fragment mapping:
 * Node 2: release_year < 2010  => games_before_2010
 * Node 3: release_year >= 2010 => games_2010_and_after
 * Node 1: Central Master       => games
 */
function getFragmentInfo(releaseYear) {
  const year = parseInt(releaseYear, 10);
  if (isNaN(year)) {
    throw new Error(`Invalid release year: ${releaseYear}`);
  }

  if (year < 2010) {
    return {
      nodeKey: 'node2',
      tableName: 'games_before_2010'
    };
  } else {
    return {
      nodeKey: 'node3',
      tableName: 'games_2010_and_after'
    };
  }
}

function getNodeTableName(nodeKey) {
  switch (nodeKey) {
    case 'node2':
      return 'games_before_2010';
    case 'node3':
      return 'games_2010_and_after';
    case 'node1':
    default:
      return 'games';
  }
}

/**
 * Normalize game input from either frontend (field1..field12) or API/tests
 */
function normalizeGameInput(data, isUpdate = false) {
  if (!data || typeof data !== 'object') {
    throw new Error('Invalid input payload');
  }

  let id, name, release_date, release_year, price, positive_reviews, negative_reviews,
      user_score, metacritic_score, average_playtime_forever, average_playtime_2weeks, median_playtime_forever;

  if (isUpdate) {
    if (data.field1 !== undefined) {
      id = parseInt(data.field1, 10);
      name = data.field2;
      release_date = data.field3;
      release_year = parseInt(data.field4, 10);
      price = parseFloat(data.field5);
      positive_reviews = parseInt(data.field6, 10) || 0;
      negative_reviews = parseInt(data.field7, 10) || 0;
      user_score = parseInt(data.field8, 10) || 0;
      metacritic_score = parseInt(data.field9, 10) || 0;
      average_playtime_forever = parseInt(data.field10, 10) || 0;
      average_playtime_2weeks = parseInt(data.field11, 10) || 0;
      median_playtime_forever = parseInt(data.field12, 10) || 0;
    } else {
      id = parseInt(data.id, 10);
      name = data.name;
      release_date = data.release_date;
      release_year = parseInt(data.release_year, 10);
      price = parseFloat(data.price);
      positive_reviews = parseInt(data.positive_reviews, 10) || 0;
      negative_reviews = parseInt(data.negative_reviews, 10) || 0;
      user_score = parseInt(data.user_score, 10) || 0;
      metacritic_score = parseInt(data.metacritic_score, 10) || 0;
      average_playtime_forever = parseInt(data.average_playtime_forever, 10) || 0;
      average_playtime_2weeks = parseInt(data.average_playtime_2weeks, 10) || 0;
      median_playtime_forever = parseInt(data.median_playtime_forever, 10) || 0;
    }
  } else {
    if (data.field1 !== undefined && data.id === undefined) {
      id = null;
      name = data.field1;
      release_date = data.field2;
      release_year = parseInt(data.field3, 10);
      price = parseFloat(data.field4);
      positive_reviews = parseInt(data.field5, 10) || 0;
      negative_reviews = parseInt(data.field6, 10) || 0;
      user_score = parseInt(data.field7, 10) || 0;
      metacritic_score = parseInt(data.field8, 10) || 0;
      average_playtime_forever = parseInt(data.field9, 10) || 0;
      average_playtime_2weeks = parseInt(data.field10, 10) || 0;
      median_playtime_forever = parseInt(data.field11, 10) || 0;
    } else {
      id = data.id !== undefined && data.id !== null ? parseInt(data.id, 10) : null;
      name = data.name;
      release_date = data.release_date;
      release_year = parseInt(data.release_year, 10);
      price = parseFloat(data.price);
      positive_reviews = parseInt(data.positive_reviews, 10) || 0;
      negative_reviews = parseInt(data.negative_reviews, 10) || 0;
      user_score = parseInt(data.user_score, 10) || 0;
      metacritic_score = parseInt(data.metacritic_score, 10) || 0;
      average_playtime_forever = parseInt(data.average_playtime_forever, 10) || 0;
      average_playtime_2weeks = parseInt(data.average_playtime_2weeks, 10) || 0;
      median_playtime_forever = parseInt(data.median_playtime_forever, 10) || 0;
    }
  }

  if (release_date instanceof Date) {
    release_date = release_date.toISOString().split('T')[0];
  } else if (typeof release_date === 'string' && release_date.includes('T')) {
    release_date = release_date.split('T')[0];
  }

  return {
    id,
    name: name || 'Untitled Game',
    release_date: release_date || new Date().toISOString().split('T')[0],
    release_year: isNaN(release_year) ? new Date().getFullYear() : release_year,
    price: isNaN(price) ? 0.0 : price,
    positive_reviews,
    negative_reviews,
    user_score,
    metacritic_score,
    average_playtime_forever,
    average_playtime_2weeks,
    median_playtime_forever
  };
}

/**
 * Write record to a fragment table using idempotent UPSERT syntax
 */
async function writeRecordToTable(nodeKey, tableName, record) {
  const sql = `
    INSERT INTO ${tableName} (
      id, name, release_date, release_year, price, positive_reviews,
      negative_reviews, user_score, metacritic_score, average_playtime_forever,
      average_playtime_2weeks, median_playtime_forever
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE
      name = VALUES(name),
      release_date = VALUES(release_date),
      release_year = VALUES(release_year),
      price = VALUES(price),
      positive_reviews = VALUES(positive_reviews),
      negative_reviews = VALUES(negative_reviews),
      user_score = VALUES(user_score),
      metacritic_score = VALUES(metacritic_score),
      average_playtime_forever = VALUES(average_playtime_forever),
      average_playtime_2weeks = VALUES(average_playtime_2weeks),
      median_playtime_forever = VALUES(median_playtime_forever)
  `;

  const values = [
    record.id, record.name, record.release_date, record.release_year, record.price,
    record.positive_reviews, record.negative_reviews, record.user_score,
    record.metacritic_score, record.average_playtime_forever,
    record.average_playtime_2weeks, record.median_playtime_forever
  ];

  return await queryNode(nodeKey, sql, values);
}

/**
 * Safely initialize sequence counter:
 * Ensures current_val is at least MAX(id) from the games table.
 * Idempotent and safe across multiple concurrent backend instances.
 */
async function getNextSequenceId(tx) {
  // Check sequence counter
  const seqRows = await tx.query('SELECT current_val FROM game_id_sequence WHERE sequence_name = "game_id" FOR UPDATE');
  if (seqRows && seqRows.length > 0) {
    const nextId = seqRows[0].current_val + 1;
    await tx.query('UPDATE game_id_sequence SET current_val = current_val + 1 WHERE sequence_name = "game_id"');
    return nextId;
  }

  // Not initialized yet: compute current maximum from games table
  const maxRows = await tx.query('SELECT COALESCE(MAX(id), 0) AS maxId FROM games FOR UPDATE');
  const maxExisting = maxRows[0]?.maxId || 0;
  const initialCounter = maxExisting + 1;

  // Insert or update with GREATEST
  await tx.query(
    `INSERT INTO game_id_sequence (sequence_name, current_val)
     VALUES ('game_id', ?)
     ON DUPLICATE KEY UPDATE current_val = GREATEST(current_val, VALUES(current_val))`,
    [initialCounter]
  );
  return initialCounter;
}

/**
 * Add Record:
 * Atomically co-locates master record insertion AND pending replication log on Node 1.
 */
async function addRecord(rawData) {
  if (isSimulatedDown('node1')) {
    const error = new Error('Node1 is down, system is read-only');
    error.status = 503;
    throw error;
  }

  const record = normalizeGameInput(rawData, false);
  const txId = generateTxId();
  const fragment = getFragmentInfo(record.release_year);

  // 1. ATOMIC LOCAL TRANSACTION ON NODE 1
  const assignedId = await withTransaction('node1', async (tx) => {
    let nextId = record.id;
    if (nextId === null || nextId === undefined) {
      nextId = await getNextSequenceId(tx);
    } else {
      // If client supplied ID explicitly, ensure counter stays above it
      await tx.query(
        `INSERT INTO game_id_sequence (sequence_name, current_val)
         VALUES ('game_id', ?)`,
        [nextId]
      ).catch(() => {});
    }
    record.id = nextId;

    const sql = `
      INSERT INTO games (
        id, name, release_date, release_year, price, positive_reviews,
        negative_reviews, user_score, metacritic_score, average_playtime_forever,
        average_playtime_2weeks, median_playtime_forever
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;
    const values = [
      record.id, record.name, record.release_date, record.release_year, record.price,
      record.positive_reviews, record.negative_reviews, record.user_score,
      record.metacritic_score, record.average_playtime_forever,
      record.average_playtime_2weeks, record.median_playtime_forever
    ];
    await tx.query(sql, values);

    // Atomically log pending replication inside the SAME transaction
    await logPendingReplicationWithTx(tx, {
      txId,
      targetNode: fragment.nodeKey,
      operation: 'INSERT',
      recordId: record.id,
      payload: record
    });

    return nextId;
  });

  record.id = assignedId;

  // 2. ATTEMPT IMMEDIATE PROPAGATION TO FRAGMENT
  let propagated = false;
  try {
    if (!isSimulatedDown(fragment.nodeKey)) {
      await writeRecordToTable(fragment.nodeKey, fragment.tableName, record);
      await markReplicationApplied(txId);
      propagated = true;
    }
  } catch (err) {
    console.warn(`[Coordinator] Immediate propagation to ${fragment.nodeKey} failed. Log remains PENDING for recovery.`);
  }

  return {
    success: true,
    message: 'Record added successfully.',
    id: record.id,
    replicatedTo: fragment.nodeKey,
    queued: !propagated,
    txId
  };
}

/**
 * Update Record:
 * Atomically updates Node 1 and writes replication log entries in one local transaction.
 */
async function updateRecord(rawData) {
  if (isSimulatedDown('node1')) {
    const error = new Error('Node1 is down, system is read-only');
    error.status = 503;
    throw error;
  }

  const record = normalizeGameInput(rawData, true);
  if (!record.id) {
    const error = new Error('ID is required for update');
    error.status = 400;
    throw error;
  }

  const txIdPrimary = generateTxId();
  const txIdSecondary = generateTxId();
  let oldYear = null;
  let oldFragment = null;
  let newFragment = null;
  let isMigration = false;

  // 1. ATOMIC LOCAL TRANSACTION ON NODE 1
  await withTransaction('node1', async (tx) => {
    const existing = await tx.query('SELECT release_year FROM games WHERE id = ? FOR UPDATE', [record.id]);
    if (!existing || existing.length === 0) {
      const notFound = new Error('Record not found');
      notFound.status = 404;
      throw notFound;
    }
    oldYear = existing[0].release_year;
    oldFragment = getFragmentInfo(oldYear);
    newFragment = getFragmentInfo(record.release_year);
    isMigration = oldFragment.nodeKey !== newFragment.nodeKey;

    const updateSql = `
      UPDATE games
      SET
        name = ?,
        release_date = ?,
        release_year = ?,
        price = ?,
        positive_reviews = ?,
        negative_reviews = ?,
        user_score = ?,
        metacritic_score = ?,
        average_playtime_forever = ?,
        average_playtime_2weeks = ?,
        median_playtime_forever = ?
      WHERE id = ?
    `;

    const values = [
      record.name, record.release_date, record.release_year, record.price,
      record.positive_reviews, record.negative_reviews, record.user_score,
      record.metacritic_score, record.average_playtime_forever,
      record.average_playtime_2weeks, record.median_playtime_forever,
      record.id
    ];
    await tx.query(updateSql, values);

    if (isMigration) {
      // Step A: DELETE from old fragment
      await logPendingReplicationWithTx(tx, {
        txId: txIdPrimary,
        targetNode: oldFragment.nodeKey,
        operation: 'DELETE',
        recordId: record.id,
        payload: { id: record.id }
      });
      // Step B: INSERT into new fragment
      await logPendingReplicationWithTx(tx, {
        txId: txIdSecondary,
        targetNode: newFragment.nodeKey,
        operation: 'INSERT',
        recordId: record.id,
        payload: record
      });
    } else {
      await logPendingReplicationWithTx(tx, {
        txId: txIdPrimary,
        targetNode: newFragment.nodeKey,
        operation: 'UPDATE',
        recordId: record.id,
        payload: record
      });
    }
  });

  // 2. ATTEMPT IMMEDIATE PROPAGATION
  if (isMigration) {
    try {
      if (!isSimulatedDown(oldFragment.nodeKey)) {
        await queryNode(oldFragment.nodeKey, `DELETE FROM ${oldFragment.tableName} WHERE id = ?`, [record.id]);
        await markReplicationApplied(txIdPrimary);
      }
    } catch (err) {}

    try {
      if (!isSimulatedDown(newFragment.nodeKey)) {
        await writeRecordToTable(newFragment.nodeKey, newFragment.tableName, record);
        await markReplicationApplied(txIdSecondary);
      }
    } catch (err) {}
  } else {
    try {
      if (!isSimulatedDown(newFragment.nodeKey)) {
        await writeRecordToTable(newFragment.nodeKey, newFragment.tableName, record);
        await markReplicationApplied(txIdPrimary);
      }
    } catch (err) {}
  }

  return {
    success: true,
    message: 'Record updated successfully',
    id: record.id,
    isMigration
  };
}

/**
 * Delete Record:
 * Atomically removes from Node 1 and writes DELETE replication log in same transaction.
 */
async function deleteRecord(id) {
  if (isSimulatedDown('node1')) {
    const error = new Error('Node1 is down, system is read-only');
    error.status = 503;
    throw error;
  }

  const numId = parseInt(id, 10);
  if (isNaN(numId)) {
    const error = new Error('Invalid ID');
    error.status = 400;
    throw error;
  }

  const txId = generateTxId();
  let releaseYear = null;
  let fragment = null;

  await withTransaction('node1', async (tx) => {
    const rows = await tx.query('SELECT release_year FROM games WHERE id = ? FOR UPDATE', [numId]);
    if (!rows || rows.length === 0) {
      const notFound = new Error('Record not found');
      notFound.status = 404;
      throw notFound;
    }
    releaseYear = rows[0].release_year;
    fragment = getFragmentInfo(releaseYear);

    await tx.query('DELETE FROM games WHERE id = ?', [numId]);

    await logPendingReplicationWithTx(tx, {
      txId,
      targetNode: fragment.nodeKey,
      operation: 'DELETE',
      recordId: numId,
      payload: { id: numId }
    });
  });

  try {
    if (!isSimulatedDown(fragment.nodeKey)) {
      await queryNode(fragment.nodeKey, `DELETE FROM ${fragment.tableName} WHERE id = ?`, [numId]);
      await markReplicationApplied(txId);
    }
  } catch (err) {}

  return {
    success: true,
    message: 'Data deleted successfully',
    id: numId
  };
}

/**
 * Helper: Merge and Post-Process Reconstructed Query Results
 * Accurately calculates distributed COUNT, applies global ORDER BY, and applies global LIMIT.
 */
function mergeReconstructedResults(rawQuery, res2, res3) {
  const clean = rawQuery.trim().replace(/\s+/g, ' ');

  // 1. COUNT(*) aggregation (supports optional alias, with or without AS)
  if (/SELECT\s+COUNT\(\*\)(\s+(AS\s+)?(\w+))?/i.test(clean)) {
    const match = clean.match(/SELECT\s+COUNT\(\*\)(\s+(AS\s+)?(\w+))?/i);
    const alias = match && match[3] ? match[3] : 'count';
    const count2 = Number(res2[0]?.[alias] || res2[0]?.count || 0);
    const count3 = Number(res3[0]?.[alias] || res3[0]?.count || 0);
    return [{ [alias]: count2 + count3, count: count2 + count3 }];
  }

  // 2. Row Merge
  let combined = [...(Array.isArray(res2) ? res2 : []), ...(Array.isArray(res3) ? res3 : [])];

  // 3. Global ORDER BY
  const orderMatch = clean.match(/ORDER\s+BY\s+(\w+)\s+(ASC|DESC)/i);
  if (orderMatch) {
    const field = orderMatch[1];
    const isDesc = orderMatch[2].toUpperCase() === 'DESC';
    combined.sort((a, b) => {
      const valA = a[field] !== undefined ? a[field] : 0;
      const valB = b[field] !== undefined ? b[field] : 0;
      return isDesc ? (valB > valA ? 1 : valB < valA ? -1 : 0) : (valA > valB ? 1 : valA < valB ? -1 : 0);
    });
  }

  // 4. Global LIMIT
  const limitMatch = clean.match(/LIMIT\s+(\d+)/i);
  if (limitMatch) {
    const limit = parseInt(limitMatch[1], 10);
    combined = combined.slice(0, limit);
  }

  return combined;
}

/**
 * Execute Query with 6-State Truth Table, Freshness Lag Check & Distributed Reconstruction
 */
async function executeQuery(rawQuery, targetNodeKey = null) {
  if (!rawQuery || typeof rawQuery !== 'string') {
    throw new Error('Query parameter is required');
  }

  const isN1Down = isSimulatedDown('node1');
  const isN2Down = isSimulatedDown('node2');
  const isN3Down = isSimulatedDown('node3');

  const availableNodes = [];
  const unavailableNodes = [];
  if (!isN1Down) availableNodes.push('node1'); else unavailableNodes.push('node1');
  if (!isN2Down) availableNodes.push('node2'); else unavailableNodes.push('node2');
  if (!isN3Down) availableNodes.push('node3'); else unavailableNodes.push('node3');

  // If client explicitly selected a node and that node is healthy
  if (targetNodeKey && !isSimulatedDown(targetNodeKey)) {
    const tableName = getNodeTableName(targetNodeKey);
    let q = rawQuery.replace(/\{TABLE\}/g, tableName);
    if (targetNodeKey === 'node2' && /FROM\s+games\b/i.test(q)) q = q.replace(/FROM\s+games\b/gi, 'FROM games_before_2010');
    if (targetNodeKey === 'node3' && /FROM\s+games\b/i.test(q)) q = q.replace(/FROM\s+games\b/gi, 'FROM games_2010_and_after');
    const results = await queryNode(targetNodeKey, q);
    return {
      transactions: results,
      node: targetNodeKey,
      isComplete: targetNodeKey === 'node1',
      isPartial: targetNodeKey !== 'node1',
      availableNodes,
      unavailableNodes
    };
  }

  // 1. STATE: Node 1 is UP -> Complete dataset from Central Master
  if (!isN1Down) {
    const tableName = 'games';
    const resolvedQuery = rawQuery.replace(/\{TABLE\}/g, tableName);
    const results = await queryNode('node1', resolvedQuery);
    return {
      transactions: results,
      node: 'node1',
      isComplete: true,
      isPartial: false,
      isStale: false,
      availableNodes,
      unavailableNodes
    };
  }

  // Check for queries that cannot be safely reconstructed without Central Node
  if (/\b(GROUP\s+BY|JOIN|HAVING)\b/i.test(rawQuery)) {
    const err = new Error('Complex queries with GROUP BY, JOIN, or HAVING cannot be safely reconstructed across distributed fragments while Central Node is offline.');
    err.status = 400;
    throw err;
  }
  if (/\b(AVG|SUM|MIN|MAX)\s*\(/i.test(rawQuery)) {
    const err = new Error('Distributed aggregations (AVG, SUM, MIN, MAX) cannot be safely reconstructed across distributed fragments while Central Node is offline.');
    err.status = 400;
    throw err;
  }

  // Check replication freshness on fragments
  const hasPendingN2 = hasPendingReplications('node2');
  const hasPendingN3 = hasPendingReplications('node3');
  const isReconstructionStale = hasPendingN2 || hasPendingN3;

  // 2. STATE: Node 1 is DOWN, but BOTH Node 2 AND Node 3 are UP
  if (isN1Down && !isN2Down && !isN3Down) {
    console.log('[Coordinator] Node 1 is down. Reconstructing dataset from Node 2 + Node 3.');
    const qNode2 = rawQuery.replace(/\{TABLE\}/g, 'games_before_2010').replace(/FROM\s+games\b/gi, 'FROM games_before_2010');
    const qNode3 = rawQuery.replace(/\{TABLE\}/g, 'games_2010_and_after').replace(/FROM\s+games\b/gi, 'FROM games_2010_and_after');

    const [res2, res3] = await Promise.all([
      queryNode('node2', qNode2),
      queryNode('node3', qNode3)
    ]);

    const merged = mergeReconstructedResults(rawQuery, res2, res3);

    return {
      transactions: merged,
      node: 'reconstructed_fragments',
      isComplete: !isReconstructionStale,
      isPartial: false,
      isStale: isReconstructionStale,
      reconstructed: true,
      warning: isReconstructionStale
        ? 'Reconstructed dataset is potentially stale: unapplied replication logs exist for one or more fragments.'
        : null,
      availableNodes,
      unavailableNodes
    };
  }

  // 3. STATE: Node 1 is DOWN, Node 3 is DOWN, ONLY Node 2 is UP
  if (isN1Down && !isN2Down && isN3Down) {
    const qNode2 = rawQuery.replace(/\{TABLE\}/g, 'games_before_2010').replace(/FROM\s+games\b/gi, 'FROM games_before_2010');
    const results = await queryNode('node2', qNode2);
    return {
      transactions: results,
      node: 'node2',
      isComplete: false,
      isPartial: true,
      isStale: hasPendingN2,
      warning: 'Partial dataset: Node 1 and Node 3 are unavailable. Only games before 2010 are included.',
      availableNodes,
      unavailableNodes
    };
  }

  // 4. STATE: Node 1 is DOWN, Node 2 is DOWN, ONLY Node 3 is UP
  if (isN1Down && isN2Down && !isN3Down) {
    const qNode3 = rawQuery.replace(/\{TABLE\}/g, 'games_2010_and_after').replace(/FROM\s+games\b/gi, 'FROM games_2010_and_after');
    const results = await queryNode('node3', qNode3);
    return {
      transactions: results,
      node: 'node3',
      isComplete: false,
      isPartial: true,
      isStale: hasPendingN3,
      warning: 'Partial dataset: Node 1 and Node 2 are unavailable. Only games from 2010 onward are included.',
      availableNodes,
      unavailableNodes
    };
  }

  // 5. ALL NODES DOWN
  const err = new Error('All database nodes are currently unavailable');
  err.status = 503;
  throw err;
}

/**
 * Recovery Engine with Database-Coordinated Claiming & Monotonic FIFO Replay:
 * 1. Assigns a unique workerId.
 * 2. Conditionally claims each pending entry via claimReplicationEntry.
 * 3. Verifies affected rows before executing the replay.
 * 4. Replays idempotent SQL on recovered node.
 * 5. On success: marks APPLIED. On failure: resets to PENDING for retry.
 */
async function recoverNodeAndReplay(nodeKey) {
  setSimulatedFailure(nodeKey, false);

  const workerId = generateWorkerId();
  const pendingEntries = await getPendingReplicationsForNode(nodeKey);
  console.log(`[Recovery] Worker ${workerId} recovering ${nodeKey}. Processing ${pendingEntries.length} entries.`);

  let replayedCount = 0;
  const targetTable = getNodeTableName(nodeKey);

  for (const entry of pendingEntries) {
    // 1. Conditionally claim the entry (database-coordinated mutual exclusion)
    const claimed = await claimReplicationEntry(entry.logId, entry.txId, workerId);
    if (!claimed) {
      console.log(`[Recovery] Entry ${entry.txId} already claimed by another worker. Halting node recovery to preserve FIFO ordering.`);
      break; // Halt recovery for this node so later entries are not applied out-of-order
    }

    // 2. Execute idempotent write
    try {
      if (entry.operation === 'INSERT' || entry.operation === 'UPDATE') {
        await writeRecordToTable(nodeKey, targetTable, entry.payload);
      } else if (entry.operation === 'DELETE') {
        await queryNode(nodeKey, `DELETE FROM ${targetTable} WHERE id = ?`, [entry.recordId]);
      }

      await markReplicationApplied(entry.txId, workerId);
      replayedCount++;
    } catch (err) {
      console.error(`[Recovery] Replay failed for ${entry.txId} on ${nodeKey}:`, err.message);
      await markReplicationStatus(entry.txId, 'PENDING', workerId);
      break; // Halt recovery on failure to maintain FIFO sequence
    }
  }

  const remaining = (await getPendingReplicationsForNode(nodeKey)).length;
  return {
    success: true,
    node: nodeKey,
    replayedCount,
    remainingPending: remaining,
    workerId
  };
}

module.exports = {
  getFragmentInfo,
  getNodeTableName,
  normalizeGameInput,
  addRecord,
  updateRecord,
  deleteRecord,
  executeQuery,
  recoverNodeAndReplay
};
