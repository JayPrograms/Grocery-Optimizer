const DEFAULT_RANKING_API_URL = "http://127.0.0.1:8000";

export async function forwardOptimization(
  body: unknown,
  fetcher: typeof fetch = fetch,
): Promise<Response> {
  const baseUrl = (process.env.RANKING_API_URL || DEFAULT_RANKING_API_URL).replace(/\/$/, "");
  try {
    const response = await fetcher(`${baseUrl}/v1/optimizations`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      cache: "no-store",
    });
    return new Response(await response.text(), {
      status: response.status,
      headers: { "Content-Type": response.headers.get("Content-Type") || "application/json" },
    });
  } catch {
    return Response.json(
      { detail: "The recommendation service is unavailable. Please try again." },
      { status: 502 },
    );
  }
}

export async function POST(request: Request): Promise<Response> {
  try {
    return forwardOptimization(await request.json());
  } catch {
    return Response.json({ detail: "Invalid request body." }, { status: 400 });
  }
}
