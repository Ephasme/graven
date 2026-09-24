# graven

Event sourcing for TypeScript: typed events, pure decisions, transactional projections, a Postgres/MySQL store.

graven keeps a stream's history as an append-only log of events, and rebuilds the stream's current state from that log every time it has to make a decision. Every append goes through one runner, `decide`. It locks the stream, folds its history, calls your decision function, and writes the new events in one transaction. Inline projections run in that same transaction.

It is split into two packages:

| Package | Contents |
|---|---|
| [`@les-sherpas/graven`](packages/graven) | Events, aggregates, `decide`, projections, catch-up projectors, an in-memory store. No database. |
| [`@les-sherpas/graven-sql`](packages/graven-sql) | The store over a [Kysely](https://kysely.dev) instance, on Postgres or MySQL/MariaDB. |

## Table of Contents

- [Install](#install)
- [Usage](#usage)
- [How it works](#how-it-works)
- [Concepts](#concepts)
- [Guarantees](#guarantees)
- [SQL schema](#sql-schema)
- [API](#api)
- [Contributing](#contributing)
- [License](#license)

## Install

The packages are published to GitHub Packages under `@les-sherpas`. Point the scope at it in your `.npmrc`, with a token that has `read:packages`:

```ini
@les-sherpas:registry=https://npm.pkg.github.com/
//npm.pkg.github.com/:_authToken=${NPM_AUTH_TOKEN}
```

```sh
bun add @les-sherpas/graven @les-sherpas/graven-sql zod kysely
# plus the driver for your database
bun add pg        # Postgres
bun add mysql2    # MySQL / MariaDB
```

### Requirements

- `zod` 4 and `kysely` 0.28 are peer dependencies.
- Node 22 or later, or Bun. The runtime needs `Promise.withResolvers`.
- Each package ships both ESM and CommonJS builds.
- Postgres and MariaDB 10.6 are tested in CI. MySQL uses the same code path.

## Usage

A bank account that can be opened, credited and debited, and that refuses to go below zero.

```ts
import { z } from "zod";
import { decide, defineAggregate, defineEvent, defineProjection } from "@les-sherpas/graven";
import { createSqlEventStore } from "@les-sherpas/graven-sql";
import { Kysely, PostgresDialect } from "kysely";
import pg from "pg";

// 1. Events: one family per event type, its payload described by a zod schema.
const opened = defineEvent({
  eventType: "account.opened",
  latestRevision: 1,
  latest: z.object({ owner: z.string() }),
});
const deposited = defineEvent({
  eventType: "account.deposited",
  latestRevision: 1,
  latest: z.object({ amount: z.number().positive() }),
});
const withdrawn = defineEvent({
  eventType: "account.withdrawn",
  latestRevision: 1,
  latest: z.object({ amount: z.number().positive() }),
});

// 2. Aggregate: the state a stream folds to, and what each event does to it.
type Account = { readonly owner: string; readonly balance: number } | null;

const account = defineAggregate({
  type: "account",
  events: [opened, deposited, withdrawn],
  initialState: null as Account,
  evolve: (state, event): Account => {
    switch (event.eventType) {
      case "account.opened":
        return { owner: event.payload.owner, balance: 0 };
      case "account.deposited":
        return state && { ...state, balance: state.balance + event.payload.amount };
      case "account.withdrawn":
        return state && { ...state, balance: state.balance - event.payload.amount };
    }
  },
});

// 3. A read model, written in the same transaction as every append.
const balances = defineProjection({
  name: "account_balances",
  aggregate: account,
  apply: async ({ streamId, state, writer }) => {
    if (!state) return;
    await writer
      .insertInto("account_balances")
      .values({ id: streamId, balance: state.balance })
      .onConflict((oc) => oc.column("id").doUpdateSet({ balance: state.balance }))
      .execute();
  },
  wipe: async (writer: Kysely<any>) => {
    await writer.deleteFrom("account_balances").execute();
  },
});

// 4. The store: any Kysely over PostgresDialect or MysqlDialect.
const db = new Kysely<any>({
  dialect: new PostgresDialect({ pool: new pg.Pool({ connectionString: process.env.DATABASE_URL }) }),
});
const store = createSqlEventStore({ db, projections: [balances] });
await store.migrate(); // creates the graven_* tables; account_balances is yours to create

// 5. Decide.
await decide(store, account, "acc-1", (state) =>
  state === null
    ? { emit: [{ eventType: "account.opened" as const, payload: { owner: "ada" } }], result: null }
    : null,
);
await decide(store, account, "acc-1", () => ({
  emit: [{ eventType: "account.deposited" as const, payload: { amount: 100 } }],
  result: null,
}));

const outcome = await decide(store, account, "acc-1", (state) => {
  if (state === null || state.balance < 30) return null; // refused: nothing is appended
  return {
    emit: [{ eventType: "account.withdrawn" as const, payload: { amount: 30 } }],
    result: state.balance - 30,
  };
});

if (outcome.appended) {
  console.log(`balance ${outcome.result}, stream at version ${outcome.version}`);
  //=> balance 70, stream at version 3
} else {
  console.log(`not appended: ${outcome.reason}`); // "refused" | "no-op"
}
```

Write each emitted `eventType` with `as const`. Without it, TypeScript widens the literal to `string`, and the call no longer typechecks.

For tests, and for anything that doesn't need a database, swap step 4 for `createInMemoryStore({ projections })`. It follows the same locking, versioning and rollback rules.

## How it works

### One decision

`decide(store, aggregate, streamId, decision)` runs these steps in a single transaction:

```mermaid
sequenceDiagram
    autonumber
    participant App as Caller
    participant D as decide
    participant S as Store (one transaction)
    participant P as Inline projections

    App->>D: decide(store, aggregate, streamId, decision)
    D->>S: lock each projection of this aggregate (shared)
    D->>S: lock the stream (row lock)
    D->>S: read the stream's events
    S-->>D: stored events
    D->>D: upcast old revisions, fold with evolve → state
    D->>D: decision(state, events) → { emit, result } or null
    alt decision returned null
        D-->>App: { appended: false, reason: "refused" }
    else events proposed
        D->>D: validate payloads, drop no-ops if elideNoOps
        D->>S: append at the expected version
        loop each projection of this aggregate
            D->>P: apply(state, appended) inside a savepoint
            P-->>D: throw → savepoint rolled back, onProjectionError
        end
        D-->>App: { appended: true, result, version, events }
    end
```

If the append finds the stream at a different version than it read, the attempt is thrown away and `decide` runs it again (3 attempts by default, with backoff). Because the stream is locked for the whole transaction, this only happens with a store that doesn't implement `lockStream`. Every shipped store does.

### The write side and the read side

```mermaid
flowchart LR
    C[Command] --> DEC["decision(state, events)"]
    subgraph TX [One transaction]
        direction LR
        LOG[(Event log<br/>graven_events)] -- read stream --> FOLD["fold(evolve)"]
        FOLD -- state --> DEC
        DEC -- new events --> LOG
        DEC -- state after append --> INLINE[Inline projection]
        INLINE --> RM[(Read model)]
    end
    LOG -- global order,<br/>from a checkpoint --> CU[Catch-up projector]
    CU --> OUT[(Search index, emails,<br/>other services…)]
```

**Inline projections** run inside the append's transaction, so the read model is never behind the log. **Catch-up projectors** run later and read the whole log in global order, so a slow or failing one never blocks a write.

## Concepts

The approach is the *Decider* pattern described by Jérémie Chassaing in [Functional Event Sourcing Decider](https://thinkbeforecoding.com/post/2021/12/17/functional-event-sourcing-decider): a decision function and an evolve function, both pure, over an initial state.

### Event and event family

An event is a fact that has already happened, so its name is in the past tense: `account.opened`, `account.withdrawn`. It is never updated or deleted. An **event family** (`defineEvent`) declares one event type:

- **`eventType`**: the name stored in every row.
- **`latestRevision`**: the current version of the payload shape.
- **`latest`**: the zod schema of that shape.

Every new event is validated against `latest` before it is appended.

### Revision and upcasting

A payload shape changes over time, but rows already stored keep the shape they were written with. A family declares one parser per older revision in `previous`. Each parser reads the old shape and transforms it into the latest one:

```ts
const provisioned = defineEvent({
  eventType: "wallet.provisioned",
  latestRevision: 2,
  latest: z.object({ minutes: z.number(), source: z.string() }),
  previous: [
    {
      revision: 1,
      parse: z.object({ minutes: z.number() }).transform((v) => ({ ...v, source: "legacy" })),
    },
  ],
});
```

A row stored at revision 1 is read back in the revision-2 shape. Your code only ever handles the latest shape, and the log is never rewritten. `defineAggregate` throws if a revision below `latestRevision` has no parser.

### Stream and aggregate

A **stream** is the ordered list of events of one entity, identified by a `streamId` such as `"acc-1"`. Each event in it has a `streamVersion` 1, 2, 3, …, with no gaps.

An **aggregate** (`defineAggregate`) describes every stream of one kind:

- **`type`**: the aggregate's name, e.g. `account`.
- **`events`**: the families its streams may contain.
- **`initialState`**: the state of a stream with no events.
- **`evolve(state, event)`**: returns the state after one event.

`evolve` must be pure and must not read the clock. A replay of the same events must always produce the same state; use `event.occurredAt` when time matters.

**Fold** is `events.reduce(evolve, initialState)`: it gives the current state of a stream.

### Decision

A decision is a synchronous, pure function `(state, events) => { emit, result } | null`:

- **`emit`**: the events to append.
- **`result`**: any value you want back from `decide`.
- **`null`**: the command is refused, and nothing is appended.

It has to be synchronous because `decide` may run it more than once. An `await` inside it would repeat its side effect on every retry, so an `async` decision is rejected by the compiler. Load what the decision needs before calling `decide`, and pass it in through a closure.

`eventRevision` is set by the store, never by the decision. An emitted event that carries it, or whose payload matches another event type, doesn't compile either.

### No-op elision

With `elideNoOps: true`, `decide` drops any proposed event whose `evolve` returns the very same state object it was given. If every proposed event is dropped, `decide` returns `{ appended: false, reason: "no-op" }`. This keeps a command that repeats a known fact, such as an idempotent sync, from growing the log.

It is off by default, because some events change nothing in the state on purpose. An audit entry, for example, must still be recorded.

### Metadata

Every appended event carries metadata:

| Field | Meaning |
|---|---|
| `correlationId` | Ties together every event caused by one original request. A fresh one is generated unless you pass it. |
| `causationId` | The `eventId` of the event that caused this one, if any. |
| `actor` | Who or what acted. |

```ts
await decide(store, account, streamId, decision, {
  metadata: { correlationId: trigger.metadata.correlationId, causationId: trigger.eventId, actor: "billing-worker" },
});
```

graven never infers `causationId` for you: only the caller knows which event triggered the command.

### Inline projection

A **projection** (`defineProjection`) keeps a read model up to date with one aggregate. After each append, `decide` calls its `apply` with the stream's new state, the events just appended and the stream's version. `writer` is the transaction's own Kysely handle, so the write commits or rolls back together with the events.

Each `apply` runs inside a savepoint. If it throws, only that projection's writes are rolled back. The events are still appended, `onProjectionError` is called, and the other projections still run. A read model that failed like this is repaired by rebuilding it.

### Rebuild

`rebuild(store, { projection })` regenerates a read model from the log, in one transaction:

1. Take the projection's lock in exclusive mode. It waits for every `decide` currently running for that aggregate to commit, and holds back new ones until the rebuild ends.
2. Call `wipe` to empty the read model.
3. Fold every stream of the aggregate and call `apply` for each.

If anything throws, the whole rebuild rolls back and the read model is left as it was. A projection without `wipe` can't be rebuilt, and asking to rebuild it throws `ProjectionNotRebuildableError`.

### Catch-up projector

A **projector** (`defineProjector`) reads the whole log in global order, optionally only for one aggregate type, starting from the last position it saved (its checkpoint). Each event is applied and the checkpoint saved in the same transaction, so each committed event is applied exactly once. `apply` does not need to be idempotent as long as it writes only through `writer`; anything it sends elsewhere, such as an email, can be repeated if the transaction fails afterwards.

`startProjectors(store, projectors)` runs them on a timer and returns three controls:

- **`wake()`**: run a pass now.
- **`stop()`**: stop, after waiting for the pass in progress.
- **`status()`**: each projector's position and lag.

An event whose `apply` keeps failing blocks that projector, and nothing past it is applied. `status().lag` shows how far behind it is.

## Guarantees

- **Streams:** stream versions are consecutive, starting at 1.
- **Concurrency:** two `decide` calls on one stream run one after the other, never interleaved, because of the row lock on the stream.
- **Global positions:** they have no gaps and follow commit order. A single counter row, locked until commit, hands them out. An identity column would leave holes on rollback, and a catch-up projector would stall on such a hole forever.
- **Atomicity:** the events of one decision, and the inline projection writes that follow them, commit together or not at all.
- **No deadlock:** `decide` takes its locks in the order projections (shared), then stream, then position counter. `rebuild` takes only the projection locks, in exclusive mode. With the order fixed, the two can't deadlock.
- **Joining a transaction:** a store built over a caller's Kysely `Transaction` joins it. `decide` then commits or rolls back with the caller's own writes.
- **Time zones:** timestamps round-trip exactly, including milliseconds, whatever time zone the driver or pool is configured with.

## SQL schema

`store.migrate()` creates five tables. It can be run again safely, and is meant for startup or a deploy step. If you manage migrations yourself, copy the tables into your own migration files. To rename them, use `createSqlEventStore({ tables: { events: "my_events" } })`.

| Table | Holds |
|---|---|
| `graven_events` | The log: `global_position`, `stream_id`, `stream_version`, `aggregate_type`, `event_id`, `event_type`, `event_revision`, `payload`, `metadata`, `occurred_at`, `recorded_at`. `(stream_id, stream_version)` and `event_id` are unique. |
| `graven_streams` | One row per stream holding its current version; this is the row `decide` locks. |
| `graven_positions` | The single counter row that hands out global positions. |
| `graven_checkpoints` | One row per catch-up projector: the last position it applied. |
| `graven_locks` | One row per projection, locked shared by `decide` and exclusively by `rebuild`. |

On MySQL/MariaDB, keys are `varchar(255)`, JSON columns are `json`, and timestamps are `datetime(3)` stored in UTC. On Postgres they are `text`, `jsonb` and `timestamptz`.

## API

Every export has a TypeScript declaration and a doc comment; this section is a map of them.

**`@les-sherpas/graven`**

| Export | Purpose |
|---|---|
| `defineEvent`, `defineEvents` | Declare an event family; build a registry that validates and upcasts payloads. |
| `defineAggregate`, `fold` | Declare an aggregate; fold events into its state. |
| `decide` | Lock, fold, decide, append and project in one transaction. Options: `metadata`, `occurredAt`, `clock`, `newId`, `maxAttempts`, `initialDelayMs`, `projections`. |
| `defineProjection`, `rebuild` | Inline read model; regenerate it from the log. |
| `defineProjector`, `runProjector`, `startProjectors` | Catch-up projector; one pass; a scheduler. |
| `createInMemoryStore` | A store with no database, for tests. |
| `EventStore` | The store interface, for writing another adapter. |

Every error extends `GravenError` and has a stable `code` and a `context` object:

| Error | Thrown when |
|---|---|
| `InvalidEventPayloadError` | A payload fails its schema. `context.issues` holds the zod issues. |
| `UnknownEventTypeError`, `UnknownEventRevisionError` | A row has a type or revision the aggregate doesn't declare. |
| `DuplicateEventFamilyError` | Two families share a type, or a family's revisions are missing or out of range. |
| `DuplicateEventIdError` | An `eventId` is already in the log. |
| `StaleStreamVersionError`, `StreamVersionConflictError` | The stream moved between the read and the append. `decide` retries on both. |
| `ConcurrentModificationError` | `decide` ran out of attempts. |
| `ProjectionNotRebuildableError` | `rebuild` was asked for a projection with no `wipe`. |
| `DuplicateProjectorError` | Two projectors share a name. |

**`@les-sherpas/graven-sql`**

| Export | Purpose |
|---|---|
| `createSqlEventStore({ db, tables?, clock?, projections?, onProjectionError? })` | The store; `db` is a `Kysely` or a `Transaction`. Adds `migrate()`. |
| `resolveTableNames` | Default table names merged with overrides. Throws on a name that isn't a plain SQL identifier. |

## Contributing

Questions and bugs go in [GitHub issues](https://github.com/Les-Sherpas/graven/issues). Pull requests are welcome.

```sh
bun install
bun run check              # typecheck + unit tests, no database
bun run db:up              # Postgres on :55433, MariaDB on :53306
GRAVEN_POSTGRES_URL=postgres://graven:graven@localhost:55433/graven \
GRAVEN_MYSQL_URL=mysql://root:graven@localhost:53306/graven \
  bun run test:integration
bun run build              # ESM + CJS + declarations into packages/*/dist
```

A change that should be released comes with a changeset. Run `bun changeset`, pick the packages and the bump, and commit the file it creates. After merge, CI opens a "Version Packages" pull request. Merging that pull request publishes the new versions. Both packages are always released at the same version.

## License

UNLICENSED. Proprietary to Les Sherpas; not licensed for use outside the organization.
