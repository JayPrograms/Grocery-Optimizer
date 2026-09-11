import { fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import Home from "@/app/page";
import type { OptimizationResponse } from "@/types";

const positiveResult: OptimizationResponse = {
  calculated_at: "2026-09-11T12:00:00Z",
  assumptions: {
    currency: "CAD",
    fuel_price_per_litre: "1.60",
    road_factor: "1.2",
    distance_method: "haversine_with_road_factor",
  },
  rankings: [{
    rank: 1,
    store: {
      store_location_id: "store-1",
      chain_name: "Fresh Market",
      store_name: "Fresh Market Downtown",
      address_summary: "10 Main St",
      latitude: "43.65",
      longitude: "-79.38",
    },
    one_way_distance_km: "5.000",
    incremental_round_trip_distance_km: "10.000",
    fuel_consumed_litres: "0.850",
    matched_items: [{
      client_item_id: "item-1",
      requested_name: "milk",
      canonical_product_id: "40000000-0000-0000-0000-000000000001",
      canonical_name: "whole milk",
      deal_id: "50000000-0000-0000-0000-000000000001",
      units_needed: 1,
      item_savings: "5.00",
    }],
    unmatched_items: [],
    grocery_savings: "5.00",
    incremental_fuel_cost: "1.36",
    net_savings: "3.64",
  }],
  unmatched_items: [],
};

afterEach(() => vi.restoreAllMocks());

function fillRequiredFields() {
  fireEvent.change(screen.getByLabelText("Item 1 name"), { target: { value: "milk" } });
  fireEvent.change(screen.getByLabelText("Latitude"), { target: { value: "43.6532" } });
  fireEvent.change(screen.getByLabelText("Longitude"), { target: { value: "-79.3832" } });
}

describe("optimization page", () => {
  it("sends the API-shaped request and renders the recommendation", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify(positiveResult), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );
    render(<Home />);
    fillRequiredFields();

    fireEvent.click(screen.getByRole("button", { name: "Check if it's worth the drive" }));

    await screen.findByText("Yes — you come out ahead.");
    expect(screen.getByText("Fresh Market Downtown")).toBeInTheDocument();
    expect(screen.getByText("10.0 km")).toBeInTheDocument();
    const sentRequest = JSON.parse(String(fetchMock.mock.calls[0][1]?.body));
    expect(fetchMock).toHaveBeenCalledWith("/api/optimizations", expect.objectContaining({ method: "POST" }));
    expect(sentRequest).toEqual({
      origin: { latitude: "43.6532", longitude: "-79.3832" },
      fuel_efficiency_l_per_100km: "8.5",
      fuel_price_per_litre: "1.60",
      radius_km: "15",
      items: [{ client_item_id: "item-1", name: "milk", quantity: "1", unit: null }],
    });
  });

  it("shows a loading state while the API request is pending", () => {
    vi.spyOn(globalThis, "fetch").mockImplementation(() => new Promise(() => undefined));
    render(<Home />);
    fillRequiredFields();

    fireEvent.click(screen.getByRole("button", { name: "Check if it's worth the drive" }));

    expect(screen.getByRole("button", { name: "Checking the numbers…" })).toBeDisabled();
  });

  it("renders API errors without discarding the form", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ detail: "Ranking data is unavailable." }), {
        status: 503,
        headers: { "Content-Type": "application/json" },
      }),
    );
    render(<Home />);
    fillRequiredFields();

    fireEvent.click(screen.getByRole("button", { name: "Check if it's worth the drive" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("Ranking data is unavailable.");
    expect(screen.getByLabelText("Item 1 name")).toHaveValue("milk");
  });

  it("renders the no-recommendation state and unmatched items", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({
        ...positiveResult,
        rankings: [],
        unmatched_items: [{ client_item_id: "item-1", requested_name: "dragon fruit", reason: "no_savings_eligible_deal" }],
      }), { status: 200, headers: { "Content-Type": "application/json" } }),
    );
    render(<Home />);
    fillRequiredFields();
    fireEvent.change(screen.getByLabelText("Item 1 name"), { target: { value: "dragon fruit" } });

    fireEvent.click(screen.getByRole("button", { name: "Check if it's worth the drive" }));

    expect(await screen.findByText("No useful savings found")).toBeInTheDocument();
    expect(screen.getByText("Unmatched: dragon fruit")).toBeInTheDocument();
  });

  it("warns when the backend returns negative net savings", async () => {
    const negativeResult = {
      ...positiveResult,
      rankings: [{ ...positiveResult.rankings[0], net_savings: "-0.75" }],
    };
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify(negativeResult), { status: 200, headers: { "Content-Type": "application/json" } }),
    );
    render(<Home />);
    fillRequiredFields();

    fireEvent.click(screen.getByRole("button", { name: "Check if it's worth the drive" }));

    expect(await screen.findByText("No — the extra trip costs more than it saves.")).toBeInTheDocument();
  });
});
