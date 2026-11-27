# Database setup

## Start Postgres with pgvector (Docker)

```bash
docker run -d \
  --name secondbrain-db \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=secondbrain \
  -p 5432:5432 \
  pgvector/pgvector:pg16
```

## Apply the schema

```bash
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/secondbrain"
psql "$DATABASE_URL" -f db/schema.sql
```

## Verify

```bash
psql "$DATABASE_URL" -c "\dt"
# should list: knowledge_items, relationships
```
