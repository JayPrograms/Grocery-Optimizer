import { afterEach, describe, expect, it, vi } from "vitest";
import { forwardOptimization } from "@/app/api/optimizations/route";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

describe("optimization API proxy", () => {
  it("forwards the payload and preserves the ranking API response", async () => {
    vi.stubEnv("RANKING_API_URL", "http://ranking-api:8000/");
    const fetcher = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ rankings: [] }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );
    const payload = { fuel_price_per_litre: "1.72" };

    const response = await forwardOptimization(payload, fetcher);

    expect(fetcher).toHaveBeenCalledWith(
      "http://ranking-api:8000/v1/optimizations",
      expect.objectContaining({ method: "POST", body: JSON.stringify(payload), cache: "no-store" }),
    );
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual({ rankings: [] });
  });

  it("returns a gateway error when the ranking API cannot be reached", async () => {
    const fetcher = vi.fn().mockRejectedValue(new Error("connection refused"));

    const response = await forwardOptimization({}, fetcher);

    expect(response.status).toBe(502);
    expect(await response.json()).toEqual({
      detail: "The recommendation service is unavailable. Please try again.",
    });
  });
});
