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

export const jsonValueSchema: z.ZodType<JsonValue> = z.json();
