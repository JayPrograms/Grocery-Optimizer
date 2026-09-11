"use client";

import { FormEvent, useState } from "react";
import { requestOptimization } from "@/lib/api";
import type { OptimizationRequest, OptimizationResponse, StoreRecommendation } from "@/types";

type ItemRow = { id: string; name: string; quantity: string; unit: string };

const initialItems: ItemRow[] = [{ id: "item-1", name: "", quantity: "1", unit: "" }];

function money(value: string, currency: string): string {
  return new Intl.NumberFormat("en-CA", {
    style: "currency",
    currency,
    minimumFractionDigits: 2,
  }).format(Number(value));
}

function Recommendation({ result }: { result: OptimizationResponse }) {
  if (!result.rankings.length) {
    return (
      <section className="result empty-result" aria-live="polite">
        <p className="eyebrow">No useful savings found</p>
        <h2>There isn&apos;t a worthwhile store match for this list.</h2>
        <p>
          No nearby store had a savings-eligible deal for the submitted items. Try a different
          item name or a wider search radius.
        </p>
        {result.unmatched_items.length > 0 && (
          <p className="coverage">Unmatched: {result.unmatched_items.map((item) => item.requested_name).join(", ")}</p>
        )}
      </section>
    );
  }

  const recommendation: StoreRecommendation = result.rankings[0];
  const worthwhile = Number(recommendation.net_savings) > 0;
  const currency = result.assumptions.currency;

  return (
    <section className={`result ${worthwhile ? "worthwhile" : "not-worthwhile"}`} aria-live="polite">
      <div className="verdict">
        <p className="eyebrow">Is it worth the drive?</p>
        <h2>{worthwhile ? "Yes — you come out ahead." : "No — the extra trip costs more than it saves."}</h2>
      </div>

      <div className="store-heading">
        <div>
          <span className="result-label">Recommended store</span>
          <h3>{recommendation.store.store_name}</h3>
          <p>{recommendation.store.chain_name} · {recommendation.store.address_summary}</p>
        </div>
        <div className="net-callout">
          <span>Net savings</span>
          <strong>{money(recommendation.net_savings, currency)}</strong>
        </div>
      </div>

      <dl className="metrics">
        <div>
          <dt>Grocery savings</dt>
          <dd>{money(recommendation.grocery_savings, currency)}</dd>
        </div>
        <div>
          <dt>Extra round-trip distance</dt>
          <dd>{Number(recommendation.incremental_round_trip_distance_km).toFixed(1)} km</dd>
        </div>
        <div>
          <dt>Estimated fuel cost</dt>
          <dd>−{money(recommendation.incremental_fuel_cost, currency)}</dd>
        </div>
        <div>
          <dt>Net savings</dt>
          <dd>{money(recommendation.net_savings, currency)}</dd>
        </div>
      </dl>

      <div className="result-details">
        <p>
          Based on {money(result.assumptions.fuel_price_per_litre, currency)}/L, your vehicle,
          and an estimated {Number(recommendation.one_way_distance_km).toFixed(1)} km each way.
        </p>
        <p>
          Matched {recommendation.matched_items.length} of {recommendation.matched_items.length + recommendation.unmatched_items.length} list items.
          {recommendation.unmatched_items.length > 0 && ` Unmatched: ${recommendation.unmatched_items.map((item) => item.requested_name).join(", ")}.`}
        </p>
      </div>
    </section>
  );
}

export default function Home() {
  const [items, setItems] = useState<ItemRow[]>(initialItems);
  const [nextItemId, setNextItemId] = useState(2);
  const [latitude, setLatitude] = useState("");
  const [longitude, setLongitude] = useState("");
  const [radius, setRadius] = useState("15");
  const [efficiency, setEfficiency] = useState("8.5");
  const [fuelPrice, setFuelPrice] = useState("1.60");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<OptimizationResponse | null>(null);

  function updateItem(id: string, field: keyof Omit<ItemRow, "id">, value: string) {
    setItems((current) => current.map((item) => item.id === id ? { ...item, [field]: value } : item));
  }

  function addItem() {
    setItems((current) => [...current, { id: `item-${nextItemId}`, name: "", quantity: "1", unit: "" }]);
    setNextItemId((current) => current + 1);
  }

  function removeItem(id: string) {
    setItems((current) => current.filter((item) => item.id !== id));
  }

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setLoading(true);
    setError(null);
    setResult(null);

    const payload: OptimizationRequest = {
      origin: { latitude, longitude },
      fuel_efficiency_l_per_100km: efficiency,
      fuel_price_per_litre: fuelPrice,
      radius_km: radius,
      items: items.map((item) => ({
        client_item_id: item.id,
        name: item.name.trim(),
        quantity: item.quantity,
        unit: item.unit.trim() || null,
      })),
    };

    try {
      setResult(await requestOptimization(payload));
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "We couldn't calculate a recommendation.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <main>
      <header className="hero">
        <p className="brand">Grocery drive check</p>
        <h1>Are the grocery savings worth the extra drive?</h1>
        <p>Tell us what you need and where you&apos;re starting. We&apos;ll compare deal savings with the fuel cost of one dedicated round trip.</p>
      </header>

      <div className="workspace">
        <form onSubmit={submit} className="input-card">
          <fieldset>
            <legend><span>1</span> Grocery list</legend>
            <p className="section-help">Add up to 50 items. Leave unit blank when quantity means packages.</p>
            <div className="item-list">
              {items.map((item, index) => (
                <div className="item-row" key={item.id}>
                  <label className="item-name">
                    <span>Item {index + 1}</span>
                    <input
                      aria-label={`Item ${index + 1} name`}
                      value={item.name}
                      onChange={(event) => updateItem(item.id, "name", event.target.value)}
                      placeholder="e.g. whole milk"
                      maxLength={512}
                      required
                    />
                  </label>
                  <label>
                    <span>Quantity</span>
                    <input
                      aria-label={`Item ${index + 1} quantity`}
                      type="number"
                      value={item.quantity}
                      onChange={(event) => updateItem(item.id, "quantity", event.target.value)}
                      min="0.001"
                      step="any"
                      required
                    />
                  </label>
                  <label>
                    <span>Unit</span>
                    <input
                      aria-label={`Item ${index + 1} unit`}
                      value={item.unit}
                      onChange={(event) => updateItem(item.id, "unit", event.target.value)}
                      placeholder="kg, L, each"
                      maxLength={32}
                    />
                  </label>
                  {items.length > 1 && (
                    <button className="remove-button" type="button" onClick={() => removeItem(item.id)} aria-label={`Remove item ${index + 1}`}>Remove</button>
                  )}
                </div>
              ))}
            </div>
            {items.length < 50 && <button className="text-button" type="button" onClick={addItem}>+ Add another item</button>}
          </fieldset>

          <fieldset>
            <legend><span>2</span> Starting location</legend>
            <p className="section-help">The API estimates store distance from these coordinates; it does not use live routing.</p>
            <div className="field-grid three-columns">
              <label>
                <span>Latitude</span>
                <input aria-label="Latitude" type="number" value={latitude} onChange={(event) => setLatitude(event.target.value)} min="-90" max="90" step="any" placeholder="43.6532" required />
              </label>
              <label>
                <span>Longitude</span>
                <input aria-label="Longitude" type="number" value={longitude} onChange={(event) => setLongitude(event.target.value)} min="-180" max="180" step="any" placeholder="−79.3832" required />
              </label>
              <label>
                <span>Search radius</span>
                <div className="input-suffix"><input aria-label="Search radius" type="number" value={radius} onChange={(event) => setRadius(event.target.value)} min="0.1" max="50" step="any" required /><span>km</span></div>
              </label>
            </div>
          </fieldset>

          <fieldset>
            <legend><span>3</span> Driving cost</legend>
            <div className="field-grid two-columns">
              <label>
                <span>Vehicle fuel efficiency</span>
                <div className="input-suffix"><input aria-label="Vehicle fuel efficiency" type="number" value={efficiency} onChange={(event) => setEfficiency(event.target.value)} min="0.1" max="50" step="any" required /><span>L/100km</span></div>
              </label>
              <label>
                <span>Fuel price per litre</span>
                <div className="input-suffix"><input aria-label="Fuel price" type="number" value={fuelPrice} onChange={(event) => setFuelPrice(event.target.value)} min="0" step="0.01" required /><span>/L</span></div>
              </label>
            </div>
          </fieldset>

          <button className="submit-button" type="submit" disabled={loading}>
            {loading ? "Checking the numbers…" : "Check if it's worth the drive"}
          </button>
          <p className="privacy-note">Your list and location are used for this calculation only and are not saved.</p>
          {error && <div className="error" role="alert"><strong>Couldn&apos;t get a recommendation.</strong><span>{error}</span></div>}
        </form>

        <aside className="result-column" aria-label="Recommendation result">
          {result ? <Recommendation result={result} /> : (
            <div className="result-placeholder">
              <div className="placeholder-icon">↗</div>
              <h2>Your recommendation will appear here.</h2>
              <p>We&apos;ll show one store and the grocery savings, extra distance, fuel cost, and net savings.</p>
            </div>
          )}
        </aside>
      </div>
    </main>
  );
}
