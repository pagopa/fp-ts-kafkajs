/* eslint-disable functional/prefer-readonly-type */

import {
  BrokersFunction,
  CompressionTypes,
  ISocketFactory,
  logCreator,
  Message,
  ProducerConfig,
  RetryOptions,
  SASLOptions
} from "kafkajs";

export type ValidableKafkaProducerConfig = {
  brokers: string[] | BrokersFunction;
  ssl?: boolean;
  sasl?: SASLOptions;
  clientId?: string;
  connectionTimeout?: number;
  authenticationTimeout?: number;
  reauthenticationThreshold?: number;
  requestTimeout?: number;
  enforceRequestTimeout?: boolean;
  retry?: RetryOptions;
  socketFactory?: ISocketFactory;
  //    logLevel?: logLevel;
  logCreator?: logCreator;
} & ProducerConfig;

export type MessageFormatter<T> = (message: T) => Message;

// eslint-disable-next-line @typescript-eslint/consistent-type-definitions
export type KafkaProducerTopicConfig<T> = {
  topic: string;
  acks?: number;
  timeout?: number;
  compression?: CompressionTypes;
  readonly messageFormatter?: MessageFormatter<T>;
};
