---
"@les-sherpas/graven": minor
"@les-sherpas/graven-sql": minor
---

Fixes a projector double-apply race and a SQL savepoint rollback that left the wrong stream version cached; the in-memory store's projection lock now upgrades shared→exclusive instead of silently no-opping. The rest is structure, dead code and naming from a code-quality pass.

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
