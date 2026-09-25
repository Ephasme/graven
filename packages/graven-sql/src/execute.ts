import { InvalidStoredRowError } from "@les-sherpas/graven";
import { createQueryId, type Compilable, type Kysely, type QueryResult, type RootOperationNode } from "kysely";
import { z } from "zod";
import type { GravenTable } from "./schema.js";

/** Compiles and runs a graven query through the caller's executor: its plugins, schema and
 * transaction apply. */
export async function execute<DB>(
  db: Kysely<DB>,
  query: { toOperationNode(): RootOperationNode },
): Promise<QueryResult<unknown>> {
  const executor = db.getExecutor();
  const queryId = createQueryId();
  return executor.executeQuery(executor.compileQuery(executor.transformQuery(query.toOperationNode(), queryId), queryId));
}

/** `schema` must accept every row Kysely types the query as returning, so the decoder cannot
 * drift from the select list. */
export async function selectRows<DB, O, S extends z.core.$ZodType>(
  db: Kysely<DB>,
  table: GravenTable,
  query: { toOperationNode(): RootOperationNode } & Compilable<O>,
  schema: S & ([O] extends [z.input<S>] ? unknown : { readonly rowMismatch: O }),
): Promise<z.output<S>[]> {
  const result = z.array(schema).safeParse((await execute(db, query)).rows);
  if (!result.success) throw new InvalidStoredRowError(table, result.error.issues);
  return result.data;
}
