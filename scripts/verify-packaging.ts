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

type PackageSpec = {
  readonly name: string;
  /** Siblings this package's dist MUST import at runtime rather than inline. */
  readonly siblingDependencies: readonly string[];
};

const packages: readonly PackageSpec[] = [
  { name: "graven", siblingDependencies: [] },
  { name: "graven-sql", siblingDependencies: ["@les-sherpas/graven"] },
];

let failed = false;

for (const pkg of packages) {
  const dir = `${import.meta.dir}/../packages/${pkg.name}`;

  for (const ext of ["index.js", "index.cjs"]) {
    const path = `${dir}/dist/${ext}`;
    const file = Bun.file(path);
    if (!(await file.exists())) {
      console.error(`FAIL ${pkg.name}: ${ext} does not exist — run \`bun run build\` first`);
      failed = true;
      continue;
    }

    const text = await file.text();

    if (text.includes("workspace:")) {
      console.error(`FAIL ${pkg.name}/dist/${ext}: contains a literal "workspace:" — a raw workspace protocol string leaked into the bundle`);
      failed = true;
    }

    for (const dep of pkg.siblingDependencies) {
      if (!text.includes(dep)) {
        console.error(
          `FAIL ${pkg.name}/dist/${ext}: expected an import of "${dep}" but found none — the sibling was likely inlined instead of imported (missing --external in scripts/build.ts?)`,
        );
        failed = true;
      }
    }

    // A duplicated class definition is the concrete symptom of an inlined sibling.
    if (pkg.name !== "graven" && text.includes("class GravenError")) {
      console.error(
        `FAIL ${pkg.name}/dist/${ext}: contains "class GravenError" — the graven core was inlined into this bundle instead of staying external`,
      );
      failed = true;
    }
  }
}

if (failed) {
  console.error("\npackaging verification failed");
  process.exit(1);
}

console.log("packaging verification passed: every dependent package imports its siblings, no workspace: leaks");
