# @les-sherpas/graven-sql

Postgres and MySQL/MariaDB event store for @les-sherpas/graven, on Kysely.

The SQL store of [graven](https://github.com/Les-Sherpas/graven). It takes any [Kysely](https://kysely.dev) instance over `PostgresDialect` or `MysqlDialect`, or a caller's Kysely `Transaction` to join.

## Install

```sh
bun add @les-sherpas/graven @les-sherpas/graven-sql zod kysely pg   # or mysql2
```

Published to GitHub Packages; see [Install](https://github.com/Les-Sherpas/graven#install) for the `.npmrc` it needs.

## Usage

```ts
import { createSqlEventStore, type GravenDatabase } from "@les-sherpas/graven-sql";
import { Kysely, PostgresDialect } from "kysely";
import pg from "pg";

const db = new Kysely<GravenDatabase>({
  dialect: new PostgresDialect({ pool: new pg.Pool({ connectionString: process.env.DATABASE_URL }) }),
});
const store = createSqlEventStore({ db, projections: [] });
await store.migrate(); // creates graven_events, graven_streams, graven_positions, graven_checkpoints, graven_locks
```

Pass `store` to `decide`, `rebuild` and `startProjectors` from `@les-sherpas/graven`. The [repository README](https://github.com/Les-Sherpas/graven#readme) documents the concepts, the tables, the guarantees and the full API.

## License

UNLICENSED. Proprietary to Les Sherpas.
