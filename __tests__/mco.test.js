// __tests__/mco.test.js
// Unified Master Test Suite for GameVault Distributed Database System

const request = require('supertest');
const app = require('../backend/server2');
const {
  queryNode,
  withTransaction,
  setSimulatedFailure,
  resetMockDatabases,
  setMockMode,
  getActiveNodeKey,
  setActiveNodeKey,
  getMockDatabases
} = require('../backend/db');

const {
  clearAllLogs,
  getAllPendingReplications,
  getPendingReplicationsForNode,
  loadQueueFromDisk,
  claimReplicationEntry,
  hasPendingReplications
} = require('../backend/replicationLog');

describe('GameVault Distributed Database System - Comprehensive Master Test Suite', () => {

  beforeAll(async () => {
    setMockMode(true);
  });

  beforeEach(async () => {
    resetMockDatabases();
    clearAllLogs();
    setSimulatedFailure('node1', false);
    setSimulatedFailure('node2', false);
    setSimulatedFailure('node3', false);
    setActiveNodeKey('node1');
  });

  afterAll(async () => {
    if (app && typeof app.close === 'function') {
      await app.close();
    }
  });

  // =========================================================================
  // 1. Data Fragmentation & CRUD Operations
  // =========================================================================
  describe('1. Data Fragmentation & CRUD Operations', () => {
    test('Scenario 1: Add game before 2010 -> Replicates to Node 1 (games) and Node 2 (games_before_2010)', async () => {
      const res = await request(app).post('/add-record').send({
        name: 'Half-Life 2',
        release_date: '2004-11-16',
        release_year: 2004,
        price: 9.99
      });

      expect(res.status).toBe(200);
      const newId = res.body.id;

      const [n1] = await queryNode('node1', 'SELECT * FROM games WHERE id = ?', [newId]);
      const [n2] = await queryNode('node2', 'SELECT * FROM games_before_2010 WHERE id = ?', [newId]);
      const [n3] = await queryNode('node3', 'SELECT * FROM games_2010_and_after WHERE id = ?', [newId]);

      expect(n1).toBeDefined();
      expect(n2).toBeDefined();
      expect(n3).toBeUndefined(); // Strictly disjoint
    });

    test('Scenario 2: Add game from 2010 onward -> Replicates to Node 1 (games) and Node 3 (games_2010_and_after)', async () => {
      const res = await request(app).post('/add-record').send({
        name: 'Portal 2',
        release_date: '2011-04-19',
        release_year: 2011,
        price: 19.99
      });

      expect(res.status).toBe(200);
      const newId = res.body.id;

      const [n1] = await queryNode('node1', 'SELECT * FROM games WHERE id = ?', [newId]);
      const [n2] = await queryNode('node2', 'SELECT * FROM games_before_2010 WHERE id = ?', [newId]);
      const [n3] = await queryNode('node3', 'SELECT * FROM games_2010_and_after WHERE id = ?', [newId]);

      expect(n1).toBeDefined();
      expect(n2).toBeUndefined(); // Strictly disjoint
      expect(n3).toBeDefined();
    });

    test('Scenario 3: Cross-Fragment Migration -> Changing release_year across 2010 boundary moves record between fragments', async () => {
      const addRes = await request(app).post('/add-record').send({
        name: 'Prototype Remaster',
        release_year: 2008,
        price: 19.99
      });
      const gameId = addRes.body.id;

      // Update release_year to 2015
      const updateRes = await request(app).put('/update-record').send({
        id: gameId,
        name: 'Prototype Remaster Definitive',
        release_year: 2015,
        price: 29.99
      });

      expect(updateRes.status).toBe(200);
      expect(updateRes.body.isMigration).toBe(true);

      const [n1] = await queryNode('node1', 'SELECT * FROM games WHERE id = ?', [gameId]);
      const [n2] = await queryNode('node2', 'SELECT * FROM games_before_2010 WHERE id = ?', [gameId]);
      const [n3] = await queryNode('node3', 'SELECT * FROM games_2010_and_after WHERE id = ?', [gameId]);

      expect(n1.release_year).toBe(2015);
      expect(n2).toBeUndefined(); // Deleted from old fragment
      expect(n3).toBeDefined();   // Inserted into new fragment
      expect(n3.name).toBe('Prototype Remaster Definitive');
    });

    test('Scenario 4: Deletion removes record from master and relevant fragment with no stale copies', async () => {
      const addRes = await request(app).post('/add-record').send({ name: 'Delete Target', release_year: 2005 });
      const gameId = addRes.body.id;

      const delRes = await request(app).delete(`/delete-record/${gameId}`);
      expect(delRes.status).toBe(200);

      const [n1] = await queryNode('node1', 'SELECT * FROM games WHERE id = ?', [gameId]);
      const [n2] = await queryNode('node2', 'SELECT * FROM games_before_2010 WHERE id = ?', [gameId]);

      expect(n1).toBeUndefined();
      expect(n2).toBeUndefined();
    });
  });

  // =========================================================================
  // 2. Transaction Log Durability, Atomicity & Rollback
  // =========================================================================
  describe('2. Transaction Log Durability and Atomicity', () => {
    test('Master write and replication-log entry commit atomically in same local transaction', async () => {
      await request(app).post('/simulate-failure').send({ node: 'node2' });

      const res = await request(app).post('/add-record').send({
        name: 'Atomic Game',
        release_year: 2006,
        price: 15.00
      });

      expect(res.status).toBe(200);
      const gameId = res.body.id;

      const [n1Game] = await queryNode('node1', 'SELECT * FROM games WHERE id = ?', [gameId]);
      expect(n1Game).toBeDefined();

      const pending = await getPendingReplicationsForNode('node2');
      expect(pending.length).toBe(1);
      expect(pending[0].recordId).toBe(gameId);
      expect(pending[0].status).toBe('PENDING');
    });

    test('Rollback safety: Aborted master transaction leaves no orphan replication logs', async () => {
      let threw = false;
      try {
        await withTransaction('node1', async (tx) => {
          await tx.query(`INSERT INTO games (id, name, release_date, release_year, price) VALUES (999, 'Rollback Game', '2005-01-01', 2005, 10.00)`);
          await tx.query(`INSERT INTO replication_log (tx_id, target_node, operation, record_id, payload, status) VALUES ('tx_rollback_999', 'node2', 'INSERT', 999, '{}', 'PENDING')`);
          throw new Error('Simulated Rollback');
        });
      } catch (e) {
        threw = true;
      }
      expect(threw).toBe(true);

      const [g] = await queryNode('node1', 'SELECT * FROM games WHERE id = ?', [999]);
      expect(g).toBeUndefined();

      const logs = await getPendingReplicationsForNode('node2');
      expect(logs.find(l => l.txId === 'tx_rollback_999')).toBeUndefined();
    });
  });

  // =========================================================================
  // 3. Multi-Instance Recovery Coordination & Abandoned Locks
  // =========================================================================
  describe('3. Multi-Instance Recovery Coordination', () => {
    test('Conditional claiming prevents concurrent workers from replaying the same transaction', async () => {
      await request(app).post('/simulate-failure').send({ node: 'node2' });
      await request(app).post('/add-record').send({ name: 'Worker Contention Game', release_year: 2002 });

      const pending = await getPendingReplicationsForNode('node2');
      const entry = pending[0];

      const claimedA = await claimReplicationEntry(entry.logId, entry.txId, 'worker_A');
      const claimedB = await claimReplicationEntry(entry.logId, entry.txId, 'worker_B');

      expect(claimedA).toBe(true);
      expect(claimedB).toBe(false); // Second claim rejected!
    });

    test('Abandoned IN_PROGRESS entries are reclaimed after worker crash timeout', async () => {
      await request(app).post('/simulate-failure').send({ node: 'node2' });
      await request(app).post('/add-record').send({ name: 'Crash Reclaim Game', release_year: 2003 });

      const pending = await getPendingReplicationsForNode('node2');
      const entry = pending[0];

      await claimReplicationEntry(entry.logId, entry.txId, 'crashed_worker');

      // Age timestamp past 30s threshold
      const oldTime = new Date(Date.now() - 40000).toISOString();
      const dbEntry = getMockDatabases().node1.replication_log.get(entry.txId);
      if (dbEntry) {
        dbEntry.claimed_at = oldTime;
        dbEntry.claimedAt = oldTime;
      }
      const qEntry = getAllPendingReplications().find(e => e.txId === entry.txId);
      if (qEntry) {
        qEntry.claimedAt = oldTime;
        qEntry.claimed_at = oldTime;
      }

      const reclaimed = await claimReplicationEntry(entry.logId, entry.txId, 'recovering_worker');
      expect(reclaimed).toBe(true);
    });
  });

  // =========================================================================
  // 4. Sequence Counter Initialization & Concurrency
  // =========================================================================
  describe('4. Sequence Counter Initialization & Concurrency', () => {
    test('Sequence counter initializes above pre-existing records to prevent duplicate IDs', async () => {
      // Pre-seed games table with record ID 500
      await queryNode('node1', `INSERT INTO games (id, name, release_date, release_year, price) VALUES (500, 'Pre-existing Game', '2005-01-01', 2005, 10.00)`);

      const res = await request(app).post('/add-record').send({ name: 'Sequential New Game', release_year: 2007 });
      expect(res.status).toBe(200);
      expect(res.body.id).toBe(501); // Guaranteed to start above 500
    });

    test('Concurrent simultaneous inserts assign strictly unique monotonic IDs', async () => {
      const inserts = Array.from({ length: 8 }, (_, i) =>
        request(app).post('/add-record').send({ name: `Concurrent Game ${i + 1}`, release_year: 2018 })
      );

      const results = await Promise.all(inserts);
      const ids = results.map(r => r.body.id);
      const uniqueIds = new Set(ids);

      expect(uniqueIds.size).toBe(8);
      expect(ids.every(id => Number.isInteger(id) && id > 0)).toBe(true);
    });
  });

  // =========================================================================
  // 5. Query Completeness, Truth Table & Distributed Reconstruction
  // =========================================================================
  describe('5. Query Completeness, Freshness & Truth Table', () => {
    beforeEach(async () => {
      await request(app).post('/add-record').send({ name: 'Retro Game', release_year: 2001, price: 10.00 });
      await request(app).post('/add-record').send({ name: 'Modern Game', release_year: 2021, price: 60.00 });
    });

    test('State 1: [Up, Up, Up] -> Complete dataset served by Central Master', async () => {
      const res = await request(app).post('/query').send({ query: 'SELECT * FROM {TABLE}' });
      expect(res.status).toBe(200);
      expect(res.body.isComplete).toBe(true);
      expect(res.body.isPartial).toBe(false);
      expect(res.body.node).toBe('node1');
    });

    test('State 2: [Down, Up, Up] -> Complete reconstructed dataset when no pending logs exist', async () => {
      await request(app).post('/simulate-failure').send({ node: 'node1' });

      const res = await request(app).post('/query').send({ query: 'SELECT * FROM {TABLE}' });
      expect(res.status).toBe(200);
      expect(res.body.reconstructed).toBe(true);
      expect(res.body.isComplete).toBe(true);
      expect(res.body.isStale).toBe(false);
      expect(res.body.transactions.length).toBe(2);
    });

    test('Replication Staleness Flag: Reconstruction flags potential staleness when fragment has unapplied writes', async () => {
      // Simulate write while Node 2 was down before Node 1 failure
      await request(app).post('/simulate-failure').send({ node: 'node2' });
      await request(app).post('/add-record').send({ name: 'Pending Lag Game', release_year: 2004 });

      // Node 1 fails, Node 2 recovers (unapplied log remains)
      await request(app).post('/simulate-failure').send({ node: 'node1' });
      setSimulatedFailure('node2', false);

      const res = await request(app).post('/query').send({ query: 'SELECT * FROM {TABLE}' });
      expect(res.status).toBe(200);
      expect(res.body.reconstructed).toBe(true);
      expect(res.body.isStale).toBe(true);
      expect(res.body.isComplete).toBe(false);
      expect(res.body.warning).toContain('Reconstructed dataset is potentially stale');
    });

    test('State 3 & 4: Partial states explicitly warn client when only one fragment is reachable', async () => {
      await request(app).post('/simulate-failure').send({ node: 'node1' });
      await request(app).post('/simulate-failure').send({ node: 'node3' });

      const res = await request(app).post('/query').send({ query: 'SELECT * FROM {TABLE}' });
      expect(res.status).toBe(200);
      expect(res.body.isComplete).toBe(false);
      expect(res.body.isPartial).toBe(true);
      expect(res.body.warning).toContain('Partial dataset: Node 1 and Node 3 are unavailable');
      expect(res.body.transactions.length).toBe(1);
    });

    test('Reconstruction merges distributed COUNT(*) aggregations correctly', async () => {
      await request(app).post('/simulate-failure').send({ node: 'node1' });

      const res = await request(app).post('/query').send({ query: 'SELECT COUNT(*) AS count FROM {TABLE}' });
      expect(res.status).toBe(200);
      expect(res.body.transactions[0].count).toBe(2);
    });

    test('Reconstruction applies global ORDER BY and LIMIT across merged fragments', async () => {
      await request(app).post('/simulate-failure').send({ node: 'node1' });

      const res = await request(app).post('/query').send({ query: 'SELECT * FROM {TABLE} ORDER BY price DESC LIMIT 1' });
      expect(res.status).toBe(200);
      expect(res.body.transactions.length).toBe(1);
      expect(res.body.transactions[0].name).toBe('Modern Game'); // Highest price ($60.00)
    });

    test('Unsupported queries (GROUP BY/JOIN) return explicit 400 error during reconstruction', async () => {
      await request(app).post('/simulate-failure').send({ node: 'node1' });

      const res = await request(app).post('/query').send({ query: 'SELECT release_year, COUNT(*) FROM {TABLE} GROUP BY release_year' });
      expect(res.status).toBe(400);
      expect(res.body.error).toContain('Complex queries with GROUP BY, JOIN, or HAVING cannot be safely reconstructed');
    });
  });

  // =========================================================================
  // 6. Recovery Synchronization & Replay Idempotency
  // =========================================================================
  describe('6. Recovery Synchronization & Replay Idempotency', () => {
    test('Recovery replays strictly in FIFO sequence and duplicate replays are idempotent', async () => {
      await request(app).post('/simulate-failure').send({ node: 'node2' });

      const g1 = await request(app).post('/add-record').send({ name: 'Sequence Game 1', release_year: 2002 });
      const g2 = await request(app).post('/add-record').send({ name: 'Sequence Game 2', release_year: 2003 });

      const pending = await getPendingReplicationsForNode('node2');
      expect(pending[0].recordId).toBe(g1.body.id);
      expect(pending[1].recordId).toBe(g2.body.id);
      expect(pending[0].logId).toBeLessThan(pending[1].logId);

      // Replay recovery
      const rec = await request(app).post('/simulate-recovery').send({ node: 'node2' });
      expect(rec.status).toBe(200);
      expect(rec.body.replayedCount).toBe(2);

      // Duplicate recovery trigger
      const dupRec = await request(app).post('/simulate-recovery').send({ node: 'node2' });
      expect(dupRec.status).toBe(200);

      const rowsA = await queryNode('node2', 'SELECT * FROM games_before_2010 WHERE id = ?', [g1.body.id]);
      expect(rowsA.length).toBe(1);
    });

    test('Pending transactions survive backend restart from durable disk backup', async () => {
      await request(app).post('/simulate-failure').send({ node: 'node3' });
      const res = await request(app).post('/add-record').send({ name: 'Restart Game', release_year: 2024 });

      // Simulate backend restart by reloading disk backup
      loadQueueFromDisk();

      const pendingAfterRestart = await getPendingReplicationsForNode('node3');
      expect(pendingAfterRestart.some(e => e.recordId === res.body.id)).toBe(true);

      const rec = await request(app).post('/simulate-recovery').send({ node: 'node3' });
      expect(rec.status).toBe(200);

      const [n3Game] = await queryNode('node3', 'SELECT * FROM games_2010_and_after WHERE id = ?', [res.body.id]);
      expect(n3Game).toBeDefined();
    });
  });
});
