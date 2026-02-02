import { AdEventSchema } from "../schemas";

describe("AdEventSchema", () => {
  it("accepts valid event", () => {
    const r = AdEventSchema.safeParse({
      event_id: "addd-fgfg-gfgg-fgfg-gfgf",
      type: "impression",
      ad_id: "ad_123",
      ts: "2026-02-01T12:34:56.789Z"
    });

    expect(r.success).toBe(true);
  });

  it("rejects missing ad_id", () => {
    const r = AdEventSchema.safeParse({
      event_id: "addd-fgfg-gfgg-fgfg-gfgf",
      type: "click",
      ts: "2026-02-01T12:34:56.789Z"
    });

    expect(r.success).toBe(false);
  });
});
