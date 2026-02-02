import { bucketMinute } from "../db";

describe("bucketMinute", () => {
  it("rounds down to minute in UTC", () => {
    expect(bucketMinute("2026-02-01T12:34:56.789Z")).toBe("2026-02-01T12:34:00.000Z");
  });

  it("keeps already bucketed timestamps", () => {
    expect(bucketMinute("2026-02-01T12:34:00.000Z")).toBe("2026-02-01T12:34:00.000Z");
  });
});