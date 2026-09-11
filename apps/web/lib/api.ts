import type { OptimizationRequest, OptimizationResponse } from "@/types";

type ValidationIssue = { msg?: string };

function apiErrorMessage(body: unknown): string {
  if (body && typeof body === "object" && "detail" in body) {
    const detail = (body as { detail: unknown }).detail;
    if (typeof detail === "string") return detail;
    if (Array.isArray(detail)) {
      const messages = detail
        .map((issue: ValidationIssue) => issue?.msg)
        .filter((message): message is string => Boolean(message));
      if (messages.length) return messages.join(" ");
    }
  }
  return "We couldn't calculate a recommendation. Please try again.";
}

export async function requestOptimization(
  payload: OptimizationRequest,
  signal?: AbortSignal,
): Promise<OptimizationResponse> {
  const response = await fetch("/api/optimizations", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    signal,
  });

  const body: unknown = await response.json().catch(() => null);
  if (!response.ok) throw new Error(apiErrorMessage(body));
  return body as OptimizationResponse;
}
