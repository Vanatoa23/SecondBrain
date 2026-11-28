# Database setup

## Recommended: Docker Compose (persistent volume + auto-schema)

From the repository root:

```bash
cp .env.example .env   # optional: customize password/port
docker compose up -d
docker compose ps      # wait until healthy
```

This binds `127.0.0.1:5432` only, stores data in the named volume `secondbrain_pgdata`
(so recreating the container never wipes your knowledge base), and applies `db/schema.sql`
automatically on first initialization.

## Alternative: manual `docker run`

```bash
docker run -d \
  --name secondbrain-db \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=secondbrain \
  -v secondbrain_pgdata:/var/lib/postgresql/data \
  -p 127.0.0.1:5432:5432 \
  pgvector/pgvector:pg16
```

## Apply (or re-apply) the schema

`db/schema.sql` is idempotent (`IF NOT EXISTS`):

```bash
export DATABASE_URL="postgresql://postgres:postgres@127.0.0.1:5432/secondbrain"
psql "$DATABASE_URL" -f db/schema.sql
```

## Verify

```bash
psql "$DATABASE_URL" -c "\dt"
# should list: knowledge_items, relationships

# Or run the read-only health check:
python scripts/doctor.py
```
