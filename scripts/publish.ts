#!/usr/bin/env bun
/**
 * Publishes every package whose current version is not yet in the registry, in dependency
 * order. `bun publish`, not `changeset publish`: Changesets publishes a Bun workspace with npm,
 * which ships `workspace:*` verbatim instead of the sibling's version.
 *
 * Under changesets/action, each package published is reported as a `git-tag` line in the
 * NDJSON file named by `CHANGESETS_OUTPUT`; the action creates tags and GitHub releases only
 * from that file.
 */
import { appendFile } from "node:fs/promises";
import { z } from "zod";
import { packages } from "./packages.js";

async function run(cmd: string[], cwd: string): Promise<{ exitCode: number; stderr: string }> {
  const proc = Bun.spawn(cmd, { cwd, stdout: "inherit", stderr: "pipe" });
  const stderr = await new Response(proc.stderr).text();
  process.stderr.write(stderr);
  return { exitCode: await proc.exited, stderr };
}

async function isPublished(name: string, version: string, cwd: string): Promise<boolean> {
  const { exitCode, stderr } = await run(["npm", "view", `${name}@${version}`, "version"], cwd);
  if (exitCode === 0) return true;
  if (stderr.includes("E404")) return false;
  throw new Error(`cannot tell whether ${name}@${version} is published (npm view exited ${exitCode})`);
}

const output = process.env.CHANGESETS_OUTPUT;

for (const dir of packages.map(({ name }) => `${import.meta.dir}/../packages/${name}`)) {
  const { name, version } = z.object({ name: z.string(), version: z.string() }).parse(await Bun.file(`${dir}/package.json`).json());
  if (await isPublished(name, version, dir)) {
    console.log(`${name}@${version} already published`);
    continue;
  }
  console.log(`\n=== publishing ${name}@${version} ===`);
  const { exitCode } = await run(["bun", "publish"], dir);
  if (exitCode !== 0) throw new Error(`bun publish failed for ${name}@${version} (exit ${exitCode})`);
  if (output) {
    await appendFile(output, `${JSON.stringify({ type: "git-tag", tag: `${name}@${version}`, packageName: name })}\n`);
  }
}
