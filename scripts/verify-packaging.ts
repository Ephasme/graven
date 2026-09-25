#!/usr/bin/env bun
/**
 * Asserts every package's built `dist/index.js` imports its declared `@les-sherpas/*`
 * dependencies rather than inlining them. `bun build --packages external` alone does not
 * guarantee this: each package's dev-time `tsconfig.json` maps `@les-sherpas/*` to a
 * sibling's `src/index.ts` via `paths`, so a build invocation missing `--external
 * '@les-sherpas/*'` silently duplicates the sibling's source (and its classes) into the
 * bundle instead of importing it — breaking `instanceof` across the package boundary for a
 * published consumer. See `scripts/build.ts`.
 */

import { packages, type PackageSpec } from "./packages.js";

async function checkBundle(pkg: PackageSpec, bundle: string): Promise<readonly string[]> {
  const path = `${import.meta.dir}/../packages/${pkg.name}/dist/${bundle}`;
  const file = Bun.file(path);
  if (!(await file.exists())) {
    return [`FAIL ${pkg.name}: ${bundle} does not exist — run \`bun run build\` first`];
  }

  const text = await file.text();
  const messages: string[] = [];

  if (text.includes("workspace:")) {
    messages.push(`FAIL ${pkg.name}/dist/${bundle}: contains a literal "workspace:" — a raw workspace protocol string leaked into the bundle`);
  }

  for (const dep of pkg.siblingDependencies) {
    if (!text.includes(dep)) {
      messages.push(
        `FAIL ${pkg.name}/dist/${bundle}: expected an import of "${dep}" but found none — the sibling was likely inlined instead of imported (missing --external in scripts/build.ts?)`,
      );
    }
  }

  // A duplicated class definition is the concrete symptom of an inlined sibling.
  if (pkg.name !== "graven" && text.includes("class GravenError")) {
    messages.push(
      `FAIL ${pkg.name}/dist/${bundle}: contains "class GravenError" — the graven core was inlined into this bundle instead of staying external`,
    );
  }

  return messages;
}

let failed = false;

for (const pkg of packages) {
  for (const bundle of ["index.js", "index.cjs"]) {
    const messages = await checkBundle(pkg, bundle);
    for (const message of messages) console.error(message);
    if (messages.length > 0) failed = true;
  }
}

if (failed) {
  console.error("\npackaging verification failed");
  process.exit(1);
}

console.log("packaging verification passed: every dependent package imports its siblings, no workspace: leaks");
