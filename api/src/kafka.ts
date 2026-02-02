import { Kafka } from "kafkajs";

const brokers = (process.env.KAFKA_BROKERS ?? "localhost:9092").split(",");
const clientId = process.env.KAFKA_CLIENT_ID ?? "ad-api";

export const TOPIC = process.env.KAFKA_TOPIC ?? "ad-events";

export const kafka = new Kafka({ clientId, brokers });
export const producer = kafka.producer({ allowAutoTopicCreation: true });
