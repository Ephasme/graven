---
"@les-sherpas/graven": patch
---

Fix TS2589 in `defineEvent`/`defineEvents` when zod's `ZodType` is augmented (e.g. by `@hono/zod-openapi`); schema bounds now use `z.core.$ZodType`.
