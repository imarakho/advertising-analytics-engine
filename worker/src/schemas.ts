import { z } from "zod";

export const EnrichedAdEventSchema = z.object({
  event_id: z.string().min(1),
  type: z.enum(["impression", "click"]),
  ad_id: z.string().min(1),
  campaign_id: z.string().optional(),
  user_id: z.string().optional(),
  ts: z.iso.datetime(),
  received_at: z.iso.datetime()
});

export type EnrichedAdEventInput = z.infer<typeof EnrichedAdEventSchema>;
