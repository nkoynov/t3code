import { MAX_WEBHOOK_DELIVERY_AGE_MINUTES } from "@t3tools/contracts";

/** Prompt a new webhook task starts with: the whole request, which the user can narrow down. */
export const DEFAULT_WEBHOOK_PROMPT = "Handle this webhook:\n{{request}}";

/** Blank means "no limit"; undefined means the input is not a valid limit, which blocks saving. */
export function parseMaxDeliveryAge(value: string): number | null | undefined {
  if (value.trim() === "") return null;
  const minutes = Number(value.trim());
  return Number.isInteger(minutes) && minutes > 0 && minutes <= MAX_WEBHOOK_DELIVERY_AGE_MINUTES
    ? minutes
    : undefined;
}
