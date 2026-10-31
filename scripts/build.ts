#!/usr/bin/env bun
/**
 * Builds every package in dependency order: two Bun bundles (esm + cjs) plus a
 * `tsc -p tsconfig.build.json` pass for declarations. `-p`, not `--build`: `--build`
 * demands `composite` projects, which the base tsconfig deliberately does not set.
 */

import { packages } from "./packages.js";

async function run(cmd: string[], cwd: string): Promise<void> {
  const proc = Bun.spawn(cmd, { cwd, stdout: "inherit", stderr: "inherit" });
  const exitCode = await proc.exited;
  if (exitCode !== 0) {
    throw new Error(`command failed (${exitCode}): ${cmd.join(" ")} (cwd=${cwd})`);
  }
}

for (const { name } of packages) {
  const dir = `${import.meta.dir}/../packages/${name}`;
  console.log(`\n=== building ${name} ===`);

  await run(["rm", "-rf", "dist"], dir);

  await run(
    [
      "bun",
      "build",
      "src/index.ts",
      "--outfile",
      "dist/index.js",
      "--target",
      "node",
      "--format",
      "esm",
      "--packages",
      "external",
      // `--packages external` only externalizes modules resolved through node_modules; each
      // package's tsconfig `paths` maps `@ephasme/*` to a sibling's `src/index.ts`, so Bun
      // otherwise follows that mapping and inlines the sibling's source (and its classes)
      // into this bundle. `--external` forces the pattern external regardless of how it
      // resolves, so the emitted bundle imports the sibling instead of duplicating it.
      "--external",
      "@ephasme/*",
    ],
    dir,
  );

  await run(
    [
      "bun",
      "build",
      "src/index.ts",
      "--outfile",
      "dist/index.cjs",
      "--target",
      "node",
      "--format",
      "cjs",
      "--packages",
      "external",
      "--external",
      "@ephasme/*",
    ],
    dir,
  );

  await run(["bunx", "tsc", "-p", "tsconfig.build.json"], dir);
}

console.log("\nbuild complete");
