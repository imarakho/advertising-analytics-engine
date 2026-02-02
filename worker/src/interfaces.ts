export type EventType = "impression" | "click";

export interface AdEvent {
  event_id: string;
  type: EventType;
  ad_id: string;
  campaign_id?: string;
  user_id?: string;
  ts: string;
}

export interface EnrichedAdEvent extends AdEvent {
  received_at: string;
}
