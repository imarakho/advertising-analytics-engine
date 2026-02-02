import { producer, TOPIC } from "./kafka";
import { EnrichedAdEvent } from "./interfaces";

const sleep = (ms: number): Promise<void> => {
  return new Promise((r) => setTimeout(r, ms));
}

// randomise delay time to about 20%
const withJitter = (ms: number): number => {
  const jitter = Math.floor(Math.random() * Math.max(1, ms * 0.2));
  return ms + jitter;
}

export const publishWithRetry = async (event: EnrichedAdEvent): Promise<void> => {
    const retries = Number(process.env.KAFKA_SEND_RETRIES ?? 5);
    const baseDelay = Number(process.env.KAFKA_SEND_BASE_DELAY_MS ?? 50);
    const maxDelay = Number(process.env.KAFKA_SEND_MAX_DELAY_MS ?? 1000);
  
    const maxAttempts = retries + 1;
  
    let lastError: unknown;
  
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        await producer.send({
          topic: TOPIC,
          messages: [{ key: event.event_id, value: JSON.stringify(event) }]
        });
        return;
      } catch (err) {
        lastError = err;
  
        if (attempt === maxAttempts) break;
  
        const delay = Math.min(maxDelay, baseDelay * 2 ** (attempt - 1));
        await sleep(withJitter(delay));
      }
    }
  
    throw lastError;
}
