// Quick test of snapshot functionality
const fs = require('fs');
const path = require('path');

const testFile = path.join(__dirname, 'test-project.json');

// Create a test project
const project = {
  version: 1,
  name: "test",
  environments: ["dev", "test", "prod"],
  state: { catalogs: [] },
  ops: [
    { id: "op_1", ts: "2025-01-01T00:00:00Z", op: "add_catalog", target: "cat_1", payload: {} },
    { id: "op_2", ts: "2025-01-01T00:01:00Z", op: "add_schema", target: "sch_1", payload: {} }
  ],
  snapshots: [
    {
      id: "snap_1",
      version: "v0.1.0",
      name: "Test snapshot",
      ts: "2025-01-01T00:00:00Z",
      state: { catalogs: [] },
      opsIncluded: ["op_1"],
      previousSnapshot: null,
      hash: "test",
      tags: []
    }
  ],
  deployments: [],
  settings: {
    autoIncrementVersion: true,
    versionPrefix: "v",
    requireSnapshotForProd: true,
    allowDrift: false,
    requireComments: false,
    warnOnBreakingChanges: true
  },
  lastSnapshotHash: "test"
};

// Write file
fs.writeFileSync(testFile, JSON.stringify(project, null, 2));
console.log('Test project written to:', testFile);

// Read it back
const read = JSON.parse(fs.readFileSync(testFile, 'utf8'));
console.log('Snapshots in file:', read.snapshots.length);
console.log('First snapshot:', read.snapshots[0].version);

// Clean up
fs.unlinkSync(testFile);
console.log('Test passed!');
