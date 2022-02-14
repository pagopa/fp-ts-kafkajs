import { KafkaConfig, Message, ProducerConfig, ProducerRecord } from "kafkajs";

export type ValidableKafkaProducerConfig = KafkaConfig & ProducerConfig;

export type MessageFormatter<T> = (message: T) => Message;

export type KafkaProducerTopicConfig<T> = Omit<ProducerRecord, "messages"> & {
  readonly messageFormatter?: MessageFormatter<T>;
};

export type KafkaProducerCompactConfig<T> = ValidableKafkaProducerConfig &
  KafkaProducerTopicConfig<T>;
