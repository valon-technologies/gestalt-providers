class Database {
  constructor() {
    throw new Error(
      "agent/cursor packages Cursor SDK with in-memory platform stores; sqlite3 Database should not be constructed",
    );
  }
}

const sqlite3 = {
  Database,
  cached: { Database },
  verbose() {
    return sqlite3;
  },
};

module.exports = sqlite3;
