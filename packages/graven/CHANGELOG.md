# @les-sherpas/graven

## 0.4.0

### Minor Changes

- bdede16: Republished under @ephasme, MIT.

## 0.3.2

No changes in this release.

## 0.3.1

### Patch Changes

- a22242c: Fix TS2589 in `defineEvent`/`defineEvents` when zod's `ZodType` is augmented (e.g. by `@hono/zod-openapi`); schema bounds now use `z.core.$ZodType`.

## 0.3.0

### Minor Changes

- 0014a66: Fixes a projector double-apply race and a SQL savepoint rollback that left the wrong stream version cached; the in-memory store's projection lock now upgrades shared→exclusive instead of silently no-opping. The rest is structure, dead code and naming from a code-quality pass.
  
  Breaking changes:
  
  - `DuplicateEventFamilyError` replaced by `InvalidEventFamilyError`, which carries a `defect: "duplicate-type" | "duplicate-revision" | "revision-out-of-range" | "missing-upcaster"` instead of one undifferentiated message.
  - `EventRegistry.parseStored` takes a `StoredEvent` instead of a loose `{ eventType, eventRevision, payload, occurredAt }`; `EventRegistry.types` and `.latestRevision` are removed.
  - `Fold`/`SealedFold` renamed to `StateWithEvents`/`SealedStateWithEvents`.
  - `EventStore<W>` is split: `lockStream`, `lockProjectionShared`, `lockProjectionExclusive`, `lockCheckpoint`, and `savepoint` moved to a new `EventStoreTransaction<W>`, the type `transaction`'s callback receives. They are no longer callable on the root store returned by `createInMemoryStore`/`createSqlEventStore`.
  - `EventStore.readStreams` and `ReadStreamOptions` removed; `readStream` no longer takes a version range.
  - `DecideOptions.projections` renamed to `runInlineProjections`.
  - `ProjectorContext.event` renamed to `.stored`.
  - `graven-sql` no longer exports `GravenTable`.
  - `InMemoryWriter`'s constructor now takes `() => TableMap<T>` instead of a `TableMap<T>`; a direct caller (a custom store, a test harness) must pass a thunk.
  
  No compatibility aliases ship for any of the above.

## 0.2.0

### Minor Changes

- c0fa67e: Strict typing end to end: no `any`, no type assertion except `as const`, no non-null assertion, no `@ts-ignore`; every value crossing an I/O boundary (SQL rows, JSON columns, stored events, `package.json` in scripts) is decoded by a zod schema. A type-aware lint gate runs in `bun run check`.
  
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

## 0.1.1

### Patch Changes

- 36cd4f7: Add package READMEs and descriptions. Require Node 22, which `Promise.withResolvers` needs.
