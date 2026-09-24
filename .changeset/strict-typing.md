---
"@les-sherpas/graven": minor
"@les-sherpas/graven-sql": minor
---

Strict typing end to end: no `any`, no type assertion except `as const`, no non-null assertion, no `@ts-ignore`; every value crossing an I/O boundary (SQL rows, JSON columns, stored events, `package.json` in scripts) is decoded by a zod schema. A type-aware lint gate runs in `bun run check`.

Breaking changes:

- `EventFamily<T, S>` takes its payload schema as a generic; payloads must survive JSON, so a `z.date()` payload no longer compiles. `BaseEvent.payload` and `EventToAppend.payload` narrow from `unknown` to `JsonValue`, which also affects custom `EventStore` implementations and direct `tx.append` callers.
- `InsertableEvent` removed; `ParsedInsertable` is `{ eventRevision, event }`.
- `Aggregate` gains `seal`/`unseal`; `Projection<W>` is an opaque interface. `decide` rejects a projection built over a different `Aggregate` object with `ProjectionAggregateMismatchError`.
- `defineAggregate` infers its state from `evolve`; `initialState` no longer needs a cast.
- `defineProjector({ aggregate })` replaces the `aggregateType` string: it now decodes each row through the aggregate's own registry, so `apply` receives the latest, upcast payload. `ProjectorContext` takes two type arguments (`ProjectorContext<E, W>`), and `Projector.aggregateType` is a required `string | undefined`.
- `InMemoryWriter<T>.table(name)` replaces `.tables`.
- Emitted `eventType` no longer needs `as const`.
- `graven-sql` exports `GravenDatabase` and `GravenTable`: build the store's queries against `Kysely<GravenDatabase & { your_table: ... }>`.
- `zod` peer range raised to `^4.3.0` for `.exactOptional()`, which `EventMetadata` needs under `exactOptionalPropertyTypes`.
- New errors: `ProjectionAggregateMismatchError`, `InvalidStoredRowError`.
