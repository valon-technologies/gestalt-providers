# terra-east4 `index_key_ord` migration runbook

One-time migration to deploy the order-preserving IndexedDB range-scan fix
(gestalt-providers #1049) to valon-tools production.

## Context

- The new `relationaldb` provider adds an `index_key_ord` column and reads index
  ranges via `WHERE index_key_ord BETWEEN ? AND ?`. Rows with `index_key_ord IS
  NULL` are **not** matched, so existing rows must be backfilled.
- `main-db` is a **single MySQL database shared by every valon-tools app**, so
  the `_gestalt_index_entries` / `_gestalt_unique_index_entries` tables hold all
  apps' index rows. The migration must backfill all of them.
- Range reads are correct without the new index (the `WHERE` is exact); the scan
  index swap (step 7) is a performance step and is not gated.

## Artifacts (already prepared)

- Provider fix merged: gestalt-providers `6e265e7c` (#1049). Snapshot published.
- Backfill: `TestBackfillOrderedKey` on branch `chore/ordered-key-backfill-migration`
  (this file's branch). Skipped unless `GESTALT_BACKFILL_DSN` is set.
- Gated deploy PR: **toolshed #2656** — bumps the `main-db` provider pin to
  `6e265e7c`. Merge only at step 6.
- App-relief PR (independent, no migration needed): **toolshed #2627** — collapses
  the per-day rollup fan-out to one ranged read. Safe under the old provider too.

## Sequence

Pause only the **ai-spend-tracker sync** for the window. Other apps may write
`NULL`-ord rows during rollout; the step-5 mop-up pass (idempotent) clears them,
and after rollout the new provider populates the column on every write.

1. **Pause** the ai-spend-tracker scheduled sync. Snapshot / confirm a recent
   automated backup of the `main-db` CloudSQL instance.

2. **Add the column nullable** (both tables; confirm prefixed names via
   `SHOW TABLES`):
   ```sql
   ALTER TABLE _gestalt_index_entries        ADD COLUMN index_key_ord LONGBLOB NULL;
   ALTER TABLE _gestalt_unique_index_entries ADD COLUMN index_key_ord LONGBLOB NULL;
   ```
   (The old provider keeps running and writes `NULL` here — fine, it's nullable.)

3. **Backfill pass 1.** Open a Cloud SQL Auth Proxy to the `main-db` instance,
   then from this branch:
   ```bash
   cd indexeddb/relationaldb
   GESTALT_BACKFILL_DSN='mysql://USER:PASS@tcp(127.0.0.1:PORT)/DBNAME' \
     go test -run TestBackfillOrderedKey -count=1 -v .
   # if a local go.work excludes this module, prefix with GOWORK=off
   ```
   It loops until `index_key_ord IS NULL` is 0 on both tables and asserts it.

4. **Deploy the new provider:** merge toolshed #2656. Confirm the Cloud Run
   rollout completes and traffic shifts to the new revision (if it doesn't
   auto-shift, mint a fresh revision and `gcloud run services update-traffic
   ... --to-latest`). From here every write populates `index_key_ord`.

5. **Backfill pass 2 (mop-up):** rerun the step-3 command to clear any rows the
   old provider wrote during the rollout window. After the rollout completes no
   new `NULL`s are created, so this converges to 0.

6. **Resume** the ai-spend-tracker sync.

7. **Swap the scan index for performance** (both tables; correctness already
   holds — this makes the range read index-backed). A plain `CREATE INDEX` will
   not upgrade the existing 2-column index because the name already exists, so
   drop and re-add online:
   ```sql
   ALTER TABLE _gestalt_index_entries
     DROP INDEX idx__gestalt_index_entries_scan,
     ADD INDEX  idx__gestalt_index_entries_scan (store_name, index_name, index_key_ord(255)),
     ALGORITHM=INPLACE, LOCK=NONE;
   ALTER TABLE _gestalt_unique_index_entries
     DROP INDEX idx__gestalt_unique_index_entries_scan,
     ADD INDEX  idx__gestalt_unique_index_entries_scan (store_name, index_name, index_key_ord(255)),
     ALGORITHM=INPLACE, LOCK=NONE;
   ```
   (Confirm index names via `SHOW INDEX FROM <table>`.) Verify with `EXPLAIN`
   that a `bound()` range query reports `type=range` using the `_scan` index.

8. **Optional / cosmetic:** once a final check shows 0 nulls and the rollout is
   fully settled, `ALTER TABLE <t> MODIFY COLUMN index_key_ord LONGBLOB NOT NULL;`
   to match the fresh-DB DDL. Not required for correctness.

## Verify

- `getUsageRanks` over month-to-date and a quarter window returns in well under
  60s; spot-check `getUsageOverview` / `getCompanyUsage` / `getOrgUsage`.
- `SELECT COUNT(*) ... WHERE index_key_ord IS NULL` is 0 on both tables.

## Rollback

The change is additive: a nullable column, a backfill, and an index swap. If the
new provider misbehaves, revert toolshed #2656 (repin to `86a2a3b9`); the old
provider ignores `index_key_ord` entirely, so the column and index are harmless
to leave in place.
