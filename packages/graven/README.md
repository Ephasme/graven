# @ephasme/graven

Event sourcing for TypeScript: typed events, pure decisions, transactional projections.

The core of [graven](https://github.com/Ephasme/graven): event families with revision upcasting, aggregates, the `decide` runner, inline projections and rebuild, catch-up projectors, and an in-memory store. It has no database dependency; [`@ephasme/graven-sql`](https://github.com/Ephasme/graven/tree/main/packages/graven-sql) provides the Postgres/MySQL store.

## Install

```sh
bun add @ephasme/graven zod
```

Published to npm.

## Usage

```ts
import { z } from "zod";
import { createInMemoryStore, decide, defineAggregate, defineEvent } from "@ephasme/graven";

const opened = defineEvent({ eventType: "account.opened", latestRevision: 1, latest: z.object({ owner: z.string() }) });

const account = defineAggregate({
  type: "account",
  events: [opened],
  initialState: null,
  evolve: (_state, event): { owner: string } | null => ({ owner: event.payload.owner }),
});

const store = createInMemoryStore();
await decide(store, account, "acc-1", (state) =>
  state ? null : { emit: [{ eventType: "account.opened", payload: { owner: "ada" } }], result: null },
);
```

The [repository README](https://github.com/Ephasme/graven#readme) documents the concepts, the diagrams of how a decision runs, the guarantees and the full API.

## License

MIT. See [LICENSE](LICENSE).
