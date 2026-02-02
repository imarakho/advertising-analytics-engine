import { z } from "zod";

export const AdEventSchema = z.object({
  event_id: z.string().min(1),
  type: z.enum(["impression", "click"]),
  ad_id: z.string().min(1),
  campaign_id: z.string().optional(),
  user_id: z.string().optional(),
  ts: z.iso.datetime().optional()
});

export type AdEventInput = z.infer<typeof AdEventSchema>;
