# Backup & restore — Second Brain

## Backup strategy

Weekly **logical backup** with `pg_dump -Fc` (custom format) via `scripts/backup.sh`:

```bash
BACKUP_SOURCE_URL=postgresql://postgres@127.0.0.1:5433/secondbrain \
    BACKUP_DIR=./backups ./scripts/backup.sh
# writes ./backups/secondbrain_YYYYMMDD_HHMMSS.dump and prints path + size
```

Custom format (`-Fc`) is compressed, supports parallel restore (`pg_restore -j`), and is the
portable insurance layer: it can be restored into any PostgreSQL server with the pgvector
extension available, on any OS, years later.

**Important distinction (README.md Part 3): a logical `pg_dump` is NOT point-in-time
recovery (PITR).** A dump is a consistent snapshot taken at one moment; everything written
after it is lost if the disk dies. PITR requires **continuous WAL archiving plus physical
base backups (`pg_basebackup`)** — see the PostgreSQL docs:
https://www.postgresql.org/docs/18/continuous-archiving.html — which is the hardening TODO
for this project (archive WAL to cheap storage, take regular base backups, and rehearse
`pg_verifybackup` + PITR restores). Until that exists, weekly logical dumps are the
portable insurance, and restic syncs of `raw/` content-addressed originals to two locations
keep the external document store recoverable in step with the database.

## Restore procedure

```bash
# 1. target database must ALREADY exist — restore.sh never creates or drops databases
createdb secondbrain_restore

# 2. restore (drops the dumped objects first, then recreates them)
./scripts/restore.sh ./backups/secondbrain_20261125_230000.dump \
    postgresql://postgres@127.0.0.1:5433/secondbrain_restore
# or: RESTORE_TARGET_URL=... ./scripts/restore.sh ./backups/....dump
```

`pg_restore --clean --if-exists` removes the objects contained in the dump before
recreating them, so **only ever restore into a throwaway/rehearsal database** — never
into the live one — unless you are doing a deliberate, announced disaster recovery.

**Client version:** the `pg_dump`/`pg_restore` client major version should match the server major version — a pg_dump 18 archive contains `SET transaction_timeout`, which a PostgreSQL 16 server rejects on restore.

## Safety guard (locked): the test-dbname rule

`scripts/restore.sh` parses the target dbname and **refuses with exit 1 unless the name
contains a delimited `test` component** — the match is `grep -qiE '(^|_)test(_|$)'`, so
`secondbrain_test` or `test` pass, but a bare substring like `latest` does **not**.
The guard runs before any other action (including the dump-file check), and
`ALLOW_NON_TEST_RESTORE=1` bypasses it with a loud warning on stderr.
Rationale: rehearsals and tests must never be able to wipe the real
database by accident — a rehearsal script that can only run against `*_test` databases
is structurally incapable of destroying production data. The bypass exists for deliberate
restores and is noisy on purpose. `scripts/backup.sh` needs no such guard because
`pg_dump` is read-only.

## Rehearsal test

`tests/test_backup.py` runs the full rehearsal against two throwaway databases on the
same server (`backup_test` source, `restore_test` target): apply schema → ingest 2 docs →
`backup.sh` → `restore.sh` → verify row counts + full-text search → verify the refusal
guard. Run it with:

```bash
TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:5433/backup_test python tests/test_backup.py
```

It follows the locked data-safety pattern from `tests/test_e2e.py`: `TEST_DATABASE_URL`
only (no `DATABASE_URL` fallback), refuses any dbname without "test", and prints
`SKIP` + exit 0 when no test database is configured.
