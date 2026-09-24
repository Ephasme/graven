import { z } from "zod";

/** What JSON.stringify round-trips. `undefined` members are allowed because zod's
 * `.optional()` outputs them and stringify drops them. */
export type JsonValue =
  | string
  | number
  | boolean
  | null
  | readonly JsonValue[]
  | { readonly [key: string]: JsonValue | undefined };

/** Decodes the post-stringify shape: `z.json()` rejects an explicit `undefined` member, so a
 * `JsonValue` that only satisfies the type via `| undefined` does not parse. */
export const jsonValueSchema: z.ZodType<JsonValue> = z.json();
