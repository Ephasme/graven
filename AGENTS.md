# Agent Instructions

## Package Manager
- Use **bun** (version pinned in `package.json` `packageManager`): `bun install`

## Commands
| Task | Command |
|------|---------|
| Test file | `bunx --bun vitest run --project unit path/to/file.test.ts` |
| Integration test file | `bunx --bun vitest run --project integration path/to/file.test.ts` |
| Typecheck (also checks `packages/graven/test/typing.ts`) | `bun run typecheck` |
| Typecheck + unit tests | `bun run check` |
| Start / wipe test databases | `bun run db:up` / `bun run db:down` |
| Build + verify bundles | `bun run build` |
| Record a release note | `bun run changeset` |

## External References
| Need | File |
|------|------|
| Architecture: `decide` flow, inline vs catch-up projections | `README.md` → "How it works" |
| Domain terms: event family, revision/upcasting, aggregate, decision, projection | `README.md` → "Concepts" |
| Invariants the stores must keep | `README.md` → "Guarantees" |
| Store tables, `migrate()` | `README.md` → "SQL schema" |
| Public exports | `README.md` → "API" |

## Tests
- Unit: `packages/*/src/**/*.test.ts`, `packages/*/test/**/*.test.ts`.
- Integration: `packages/*/test-integration/**/*.test.ts`; needs `bun run db:up` and:
  - `GRAVEN_POSTGRES_URL=postgres://graven:graven@localhost:55433/graven`
  - `GRAVEN_MYSQL_URL=mysql://root:graven@localhost:53306/graven`
- Integration runs with `TZ=Europe/Paris` on purpose; never force UTC to make a test pass.
- `packages/graven/test/typing.ts` is compile-time only: each misuse carries `@ts-expect-error`; vitest never runs it.

## Adding or Renaming a Package
Update the package list in all of:
- `scripts/build.ts` and `scripts/publish.ts` (dependency order)
- `scripts/verify-packaging.ts` (`siblingDependencies`: runtime imports only, not `import type`)
- `tsconfig.json` `references`
- `vitest.config.ts` `alias`
- `.changeset/config.json` `fixed`
- the new package's `tsconfig.json` `paths`

## Key Conventions
- Cross-package imports use `@les-sherpas/<name>`, never relative paths into a sibling.
- Sibling deps are `"workspace:*"` in `dependencies`; `kysely` and `zod` are `peerDependencies`.
- `scripts/build.ts` MUST keep `--external '@les-sherpas/*'` on both bundles; without it siblings get inlined and `instanceof` breaks across packages.

## Release
- Never bump `version` by hand; add a changeset (`bun run changeset`). Both packages share one version (`fixed`).
- Push to `main` opens/updates the "Version Packages" PR; merging it publishes to GitHub Packages (`.github/workflows/ci.yml` `release` job).
- Publishing goes through `scripts/publish.ts`; never `changeset publish` (npm ships `workspace:*` verbatim).
