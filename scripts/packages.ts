export type PackageSpec = {
  readonly name: string;
  /** Siblings this package's dist MUST import at runtime rather than inline. */
  readonly siblingDependencies: readonly string[];
};

/** Every workspace package, in dependency order: build and publish walk it front to back. */
export const packages: readonly PackageSpec[] = [
  { name: "graven", siblingDependencies: [] },
  { name: "graven-sql", siblingDependencies: ["@les-sherpas/graven"] },
];
