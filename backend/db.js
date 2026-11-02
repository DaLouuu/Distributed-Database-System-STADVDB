// backend/db.js
// Database connection manager for GameVault Distributed Database System

const mysql = require('mysql2/promise');

// Configuration for each node with environment variable overrides
const nodeConfigs = {
  node1: {
    host: process.env.DB_NODE1_HOST || 'ccscloud.dlsu.edu.ph',
    port: parseInt(process.env.DB_NODE1_PORT, 10) || 20302,
    user: process.env.DB_NODE1_USER || 'user',
    password: process.env.DB_NODE1_PASSWORD || 'password',
    database: process.env.DB_NODE1_NAME || 'mco_db',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
    connectTimeout: 4000
  },
  node2: {
    host: process.env.DB_NODE2_HOST || 'ccscloud.dlsu.edu.ph',
    port: parseInt(process.env.DB_NODE2_PORT, 10) || 20312,
    user: process.env.DB_NODE2_USER || 'user',
    password: process.env.DB_NODE2_PASSWORD || 'password',
    database: process.env.DB_NODE2_NAME || 'mco_db',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
    connectTimeout: 4000
  },
  node3: {
    host: process.env.DB_NODE3_HOST || 'ccscloud.dlsu.edu.ph',
    port: parseInt(process.env.DB_NODE3_PORT, 10) || 20322,
    user: process.env.DB_NODE3_USER || 'user',
    password: process.env.DB_NODE3_PASSWORD || 'password',
    database: process.env.DB_NODE3_NAME || 'mco_db',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
    connectTimeout: 4000
  }
};

// Simulated failure tracking
const simulatedFailures = {
  node1: false,
  node2: false,
  node3: false
};

// Active node selection (defaults to node1)
let activeNodeKey = 'node1';

// Connection pools for real MySQL
const pools = {};

// Monotonic sequence counter for mock replication log IDs
let mockLogSequence = 1;

// In-Memory mock store for isolated testing or fallback
const mockDatabases = {
  node1: {
    games: new Map(),
    replication_log: new Map(),
    game_id_sequence: new Map()
  },
  node2: {
    games_before_2010: new Map(),
    replication_log: new Map()
  },
  node3: {
    games_2010_and_after: new Map(),
    replication_log: new Map()
  }
};

// Determine whether mock driver is active
let isMockMode = process.env.NODE_ENV === 'test' || process.env.USE_MOCK_DB === 'true';

function setMockMode(enabled) {
  isMockMode = enabled;
}

function getIsMockMode() {
  return isMockMode;
}

function getMockDatabases() {
  return mockDatabases;
}

function resetMockDatabases() {
  mockDatabases.node1.games.clear();
  mockDatabases.node1.replication_log.clear();
  mockDatabases.node1.game_id_sequence.clear();

  mockDatabases.node2.games_before_2010.clear();
  mockDatabases.node2.replication_log.clear();

  mockDatabases.node3.games_2010_and_after.clear();
  mockDatabases.node3.replication_log.clear();

  mockLogSequence = 1;
}

function getPool(nodeKey) {
  if (isMockMode) {
    return null;
  }
  if (!pools[nodeKey]) {
    const config = nodeConfigs[nodeKey];
    if (!config) throw new Error(`Unknown node: ${nodeKey}`);
    pools[nodeKey] = mysql.createPool(config);
  }
  return pools[nodeKey];
}

function isSimulatedDown(nodeKey) {
  return !!simulatedFailures[nodeKey];
}

function setSimulatedFailure(nodeKey, isDown) {
  if (!(nodeKey in simulatedFailures)) {
    throw new Error(`Invalid node key: ${nodeKey}`);
  }
  simulatedFailures[nodeKey] = isDown;
}

function getActiveNodeKey() {
  return activeNodeKey;
}

function setActiveNodeKey(nodeKey) {
  if (!nodeConfigs[nodeKey]) {
    throw new Error(`Invalid node: ${nodeKey}`);
  }
  activeNodeKey = nodeKey;
}

function getNodeConfig(nodeKey) {
  return nodeConfigs[nodeKey];
}

async function pingNode(nodeKey) {
  if (isSimulatedDown(nodeKey)) {
    return false;
  }

  if (isMockMode) {
    return true;
  }

  try {
    const pool = getPool(nodeKey);
    const conn = await pool.getConnection();
    try {
      await conn.ping();
      return true;
    } finally {
      conn.release();
    }
  } catch (err) {
    return false;
  }
}

function cloneDbState(nodeKey) {
  const db = mockDatabases[nodeKey];
  const clone = {};
  for (const [tblName, mapObj] of Object.entries(db)) {
    clone[tblName] = new Map();
    for (const [k, v] of mapObj.entries()) {
      clone[tblName].set(k, JSON.parse(JSON.stringify(v)));
    }
  }
  return clone;
}

function restoreDbState(nodeKey, snapshot) {
  const db = mockDatabases[nodeKey];
  for (const [tblName, mapObj] of Object.entries(snapshot)) {
    db[tblName] = mapObj;
  }
}

function cloneRow(obj) {
  return obj ? JSON.parse(JSON.stringify(obj)) : null;
}

async function queryNode(nodeKey, sql, params = []) {
  if (isSimulatedDown(nodeKey)) {
    const err = new Error(`Node ${nodeKey} is unavailable (simulated or offline)`);
    err.code = 'NODE_UNAVAILABLE';
    throw err;
  }

  if (isMockMode) {
    return executeMockQuery(nodeKey, sql, params);
  }

  const pool = getPool(nodeKey);
  const conn = await pool.getConnection();
  try {
    await conn.query('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ');
    const [results] = await conn.query(sql, params);
    return results;
  } finally {
    conn.release();
  }
}

async function withTransaction(nodeKey, callback) {
  if (isSimulatedDown(nodeKey)) {
    const err = new Error(`Node ${nodeKey} is unavailable (simulated or offline)`);
    err.code = 'NODE_UNAVAILABLE';
    throw err;
  }

  if (isMockMode) {
    const snapshot = cloneDbState(nodeKey);
    try {
      const mockTx = {
        query: (sql, params) => executeMockQuery(nodeKey, sql, params)
      };
      const result = await callback(mockTx);
      return result;
    } catch (err) {
      restoreDbState(nodeKey, snapshot);
      throw err;
    }
  }

  const pool = getPool(nodeKey);
  const conn = await pool.getConnection();
  try {
    await conn.query('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ');
    await conn.beginTransaction();

    const txContext = {
      query: async (sql, params) => {
        const [results] = await conn.query(sql, params);
        return results;
      }
    };

    const result = await callback(txContext);
    await conn.commit();
    return result;
  } catch (err) {
    await conn.rollback();
    throw err;
  } finally {
    conn.release();
  }
}

// In-Memory mock query engine
function executeMockQuery(nodeKey, sql, params = []) {
  const cleanSql = sql.trim().replace(/\s+/g, ' ');
  const db = mockDatabases[nodeKey];
  if (!db) throw new Error(`Mock db not found for ${nodeKey}`);

  let table = null;
  if (/FROM\s+games_before_2010|INTO\s+games_before_2010|UPDATE\s+games_before_2010|DELETE\s+FROM\s+games_before_2010/i.test(cleanSql)) {
    table = db.games_before_2010;
  } else if (/FROM\s+games_2010_and_after|INTO\s+games_2010_and_after|UPDATE\s+games_2010_and_after|DELETE\s+FROM\s+games_2010_and_after/i.test(cleanSql)) {
    table = db.games_2010_and_after;
  } else if (/FROM\s+games\b|INTO\s+games\b|UPDATE\s+games\b|DELETE\s+FROM\s+games\b/i.test(cleanSql)) {
    table = db.games;
  } else if (/replication_log/i.test(cleanSql)) {
    table = db.replication_log;
  } else if (/game_id_sequence/i.test(cleanSql)) {
    table = db.game_id_sequence;
  }

  // 1. Sequence table operations (Safe Initialization & Atomic Increment)
  if (/SELECT\s+current_val\s+FROM\s+game_id_sequence\s+WHERE\s+sequence_name\s*=\s*['"]game_id['"]/i.test(cleanSql)) {
    const row = db.game_id_sequence.get('game_id');
    return row ? [{ current_val: row.current_val }] : [];
  }
  if (/INSERT\s+INTO\s+game_id_sequence/i.test(cleanSql)) {
    const initialVal = params.length > 1 ? Number(params[1]) : Number(params[0] || 0);
    const existing = db.game_id_sequence.get('game_id');
    if (!existing) {
      db.game_id_sequence.set('game_id', { sequence_name: 'game_id', current_val: initialVal });
    } else {
      existing.current_val = Math.max(existing.current_val, initialVal);
    }
    return { affectedRows: 1 };
  }
  if (/UPDATE\s+game_id_sequence\s+SET\s+current_val\s*=\s*current_val\s*\+\s*1/i.test(cleanSql)) {
    const row = db.game_id_sequence.get('game_id');
    if (row) {
      row.current_val += 1;
      return { affectedRows: 1 };
    }
    return { affectedRows: 0 };
  }

  // 2. Replication Log - INSERT
  if (/INSERT\s+INTO\s+replication_log/i.test(cleanSql)) {
    const logId = mockLogSequence++;
    const entry = {
      log_id: logId,
      tx_id: params[0],
      target_node: params[1],
      operation: params[2],
      record_id: Number(params[3]),
      payload: typeof params[4] === 'string' ? JSON.parse(params[4]) : params[4],
      status: params[5] || 'PENDING',
      worker_id: null,
      claimed_at: null,
      created_at: new Date().toISOString()
    };
    db.replication_log.set(entry.tx_id, entry);
    return { affectedRows: 1, insertId: logId };
  }

  // 3. Conditional claim for distributed recovery
  // UPDATE replication_log SET status = 'IN_PROGRESS', worker_id = ?, claimed_at = ? WHERE log_id = ? AND (status = 'PENDING' OR (status = 'IN_PROGRESS' AND claimed_at < ?))
  if (/UPDATE\s+replication_log\s+SET\s+status\s*=\s*'IN_PROGRESS'/i.test(cleanSql)) {
    const workerId = params[0];
    const claimedAt = params[1];
    const targetLogId = Number(params[2]);
    const timeoutThreshold = params[3]; // ISO string or timestamp

    for (const entry of db.replication_log.values()) {
      if (entry.log_id === targetLogId) {
        const isPending = entry.status === 'PENDING';
        const claimTime = entry.claimed_at || entry.claimedAt;
        const isAbandoned = entry.status === 'IN_PROGRESS' && claimTime && new Date(claimTime) < new Date(timeoutThreshold);
        if (isPending || isAbandoned) {
          entry.status = 'IN_PROGRESS';
          entry.worker_id = workerId;
          entry.claimed_at = claimedAt;
          entry.claimedAt = claimedAt;
          return { affectedRows: 1 };
        }
      }
    }
    return { affectedRows: 0 };
  }

  // 4. Update status with worker verification (e.g. APPLIED or PENDING retry)
  if (/UPDATE\s+replication_log\s+SET\s+status\s*=\s*\?/i.test(cleanSql)) {
    const newStatus = params[0];
    const whereTxIdMatch = cleanSql.match(/WHERE\s+tx_id\s*=\s*\?/i);

    if (whereTxIdMatch) {
      const txId = params[params.length - 1];
      const entry = db.replication_log.get(txId);
      if (entry) {
        entry.status = newStatus;
        entry.updated_at = new Date().toISOString();
        if (newStatus === 'APPLIED') {
          entry.claimed_at = null;
        }
        return { affectedRows: 1 };
      }
      return { affectedRows: 0 };
    }
  }

  // 5. SELECT pending or abandoned logs for target node
  if (/SELECT\s+.*FROM\s+replication_log\s+WHERE\s+target_node\s*=\s*\?/i.test(cleanSql)) {
    const target = params[0];
    const rows = Array.from(db.replication_log.values())
      .filter(e => e.target_node === target && (e.status === 'PENDING' || e.status === 'IN_PROGRESS'))
      .sort((a, b) => a.log_id - b.log_id)
      .map(cloneRow);
    return rows;
  }

  // 6. SELECT MAX(id)
  if (/SELECT\s+COALESCE\(MAX\(id\),\s*0\)\s+AS\s+(lastId|maxId)|SELECT\s+MAX\(id\)\s+AS\s+(lastId|maxId)/i.test(cleanSql)) {
    let maxId = 0;
    if (table) {
      for (const id of table.keys()) {
        if (id > maxId) maxId = id;
      }
    }
    return [{ lastId: maxId, maxId: maxId }];
  }

  // 7. SELECT COUNT(*)
  if (/SELECT\s+COUNT\(\*\)\s+AS\s+count/i.test(cleanSql)) {
    return [{ count: table ? table.size : 0 }];
  }

  // 8. SELECT * FROM table WHERE id = ?
  if (/SELECT\s+\*\s+FROM\s+\w+\s+WHERE\s+id\s*=\s*\?/i.test(cleanSql)) {
    const id = Number(params[0]);
    const row = table ? table.get(id) : null;
    return row ? [cloneRow(row)] : [];
  }

  // 9. SELECT release_year FROM table WHERE id = ?
  if (/SELECT\s+.*release_year.*\s+FROM\s+\w+\s+WHERE\s+id\s*=\s*\?/i.test(cleanSql)) {
    const id = Number(params[0]);
    const row = table ? table.get(id) : null;
    return row ? [{ release_year: row.release_year, id: row.id }] : [];
  }

  // 10. General SELECT with filtering, ORDER BY, LIMIT
  if (/^SELECT/i.test(cleanSql)) {
    if (!table) return [];
    let rows = Array.from(table.values()).map(cloneRow);

    const likeMatch = cleanSql.match(/WHERE\s+(\w+)\s+LIKE\s+'%([^%]*)%'/i);
    if (likeMatch) {
      const field = likeMatch[1];
      const term = likeMatch[2].toLowerCase();
      rows = rows.filter(r => String(r[field] || '').toLowerCase().includes(term));
    }

    if (/ORDER\s+BY\s+average_playtime_forever\s+DESC/i.test(cleanSql)) {
      rows.sort((a, b) => (b.average_playtime_forever || 0) - (a.average_playtime_forever || 0));
    } else if (/ORDER\s+BY\s+user_score\s+DESC/i.test(cleanSql)) {
      rows.sort((a, b) => (b.user_score || 0) - (a.user_score || 0));
    } else if (/ORDER\s+BY\s+price\s+DESC/i.test(cleanSql)) {
      rows.sort((a, b) => (b.price || 0) - (a.price || 0));
    } else if (/ORDER\s+BY\s+id\s+ASC/i.test(cleanSql)) {
      rows.sort((a, b) => a.id - b.id);
    }

    const limitMatch = cleanSql.match(/LIMIT\s+(\d+)/i);
    if (limitMatch) {
      const limit = parseInt(limitMatch[1], 10);
      rows = rows.slice(0, limit);
    }
    return rows;
  }

  // 11. INSERT INTO table
  if (/^INSERT\s+INTO/i.test(cleanSql) || /^REPLACE\s+INTO/i.test(cleanSql)) {
    if (!table) throw new Error(`Table not found for INSERT`);
    let record;
    if (params && params.length > 0) {
      record = {
        id: Number(params[0]),
        name: params[1],
        release_date: params[2],
        release_year: Number(params[3]),
        price: parseFloat(params[4]),
        positive_reviews: Number(params[5] || 0),
        negative_reviews: Number(params[6] || 0),
        user_score: Number(params[7] || 0),
        metacritic_score: Number(params[8] || 0),
        average_playtime_forever: Number(params[9] || 0),
        average_playtime_2weeks: Number(params[10] || 0),
        median_playtime_forever: Number(params[11] || 0)
      };
    } else {
      const valuesMatch = cleanSql.match(/VALUES\s*\(([^)]+)\)/i);
      if (valuesMatch) {
        const rawTokens = valuesMatch[1].split(',').map(s => s.trim().replace(/^['"]|['"]$/g, ''));
        record = {
          id: Number(rawTokens[0]),
          name: rawTokens[1],
          release_date: rawTokens[2],
          release_year: Number(rawTokens[3]),
          price: parseFloat(rawTokens[4] || 0),
          positive_reviews: Number(rawTokens[5] || 0),
          negative_reviews: Number(rawTokens[6] || 0),
          user_score: Number(rawTokens[7] || 0),
          metacritic_score: Number(rawTokens[8] || 0),
          average_playtime_forever: Number(rawTokens[9] || 0),
          average_playtime_2weeks: Number(rawTokens[10] || 0),
          median_playtime_forever: Number(rawTokens[11] || 0)
        };
      }
    }
    if (record) {
      table.set(record.id, record);
      return { affectedRows: 1, insertId: record.id };
    }
  }

  // 12. UPDATE table SET ... WHERE id = ?
  if (/^UPDATE/i.test(cleanSql)) {
    if (!table) throw new Error(`Table not found for UPDATE`);
    const id = Number(params[params.length - 1]);
    const existing = table.get(id);
    if (!existing) {
      return { affectedRows: 0 };
    }
    const updated = {
      ...existing,
      name: params[0],
      release_date: params[1],
      release_year: Number(params[2]),
      price: parseFloat(params[3]),
      positive_reviews: Number(params[4]),
      negative_reviews: Number(params[5]),
      user_score: Number(params[6]),
      metacritic_score: Number(params[7]),
      average_playtime_forever: Number(params[8]),
      average_playtime_2weeks: Number(params[9]),
      median_playtime_forever: Number(params[10])
    };
    table.set(id, updated);
    return { affectedRows: 1 };
  }

  // 13. DELETE FROM table WHERE id = ?
  if (/^DELETE\s+FROM\s+\w+\s+WHERE\s+id\s*=\s*\?/i.test(cleanSql)) {
    if (!table) return { affectedRows: 0 };
    const id = Number(params[0]);
    const existed = table.delete(id);
    return { affectedRows: existed ? 1 : 0 };
  }

  // 14. DELETE FROM table (cleanup)
  if (/^DELETE\s+FROM\s+\w+/i.test(cleanSql)) {
    if (table) {
      const count = table.size;
      table.clear();
      return { affectedRows: count };
    }
    return { affectedRows: 0 };
  }

  return [];
}

async function closeAllPools() {
  for (const [key, pool] of Object.entries(pools)) {
    try {
      await pool.end();
    } catch (e) {
      // Ignore
    }
    delete pools[key];
  }
}

module.exports = {
  nodeConfigs,
  getPool,
  queryNode,
  withTransaction,
  pingNode,
  isSimulatedDown,
  setSimulatedFailure,
  getActiveNodeKey,
  setActiveNodeKey,
  getNodeConfig,
  setMockMode,
  getIsMockMode,
  getMockDatabases,
  resetMockDatabases,
  closeAllPools
};
