/* eslint-disable sort-keys */
import * as E from "fp-ts/Either";
import * as IO from "fp-ts/IO";
import * as O from "fp-ts/Option";
import * as TE from "fp-ts/TaskEither";
import { constVoid, pipe } from "fp-ts/function";

import {
  Consumer,
  ConsumerConfig,
  ConsumerRunConfig,
  ConsumerSubscribeTopics,
  EachBatchHandler,
  EachMessageHandler,
  Kafka,
  KafkaConfig
} from "kafkajs";
import { connect } from "./KafkaOperation";
import {
  AzureEventhubSas,
  AzureEventhubSasFromString
} from "./KafkaProducerCompact";
import { KafkaConsumerPayload } from "./KafkaTypes";

export interface IKafkaConsumerCompact {
  readonly consumer: Consumer;
}

export type KafkaConsumerCompact = IO.IO<IKafkaConsumerCompact>;

export type ValidableKafkaConsumerConfig = KafkaConfig & ConsumerConfig;

export const getConsumerFromConfig = (
  config: ValidableKafkaConsumerConfig
): KafkaConsumerCompact => (): IKafkaConsumerCompact => ({
  consumer: new Kafka(config).consumer(config)
});

export const getConsumerFromSas = (
  sas: AzureEventhubSas,
  groupId?: string
): KafkaConsumerCompact =>
  pipe(
    O.fromNullable(groupId),
    O.getOrElse(() => "consumer-group"),
    defaultGroupId => ({
      brokers: [`${sas.url}:9093`],
      ssl: true,
      sasl: {
        mechanism: "plain" as const,
        username: "$ConnectionString",
        password: AzureEventhubSasFromString.encode(sas)
      },
      clientId: sas.policy,
      groupId: defaultGroupId
    }),
    fullConfig => getConsumerFromConfig(fullConfig)
  );

export const subscribe = (subscription: ConsumerSubscribeTopics) => (
  client: Consumer
): TE.TaskEither<Error, void> =>
  TE.tryCatch(() => pipe(subscription, client.subscribe), E.toError);

export interface IConsumerRunOptions {
  readonly autoCommit?: boolean;
  readonly autoCommitInterval?: number | null;
  readonly autoCommitThreshold?: number | null;
  readonly eachBatchAutoResolve?: boolean;
  readonly partitionsConsumedConcurrently?: number;
}
export const getMessageConsumerRunConfig = (
  eachMessageHandler: EachMessageHandler,
  consumerRunOptions: IConsumerRunOptions
): ConsumerRunConfig => ({
  autoCommit: consumerRunOptions.autoCommit,
  autoCommitInterval: consumerRunOptions.autoCommitInterval,
  autoCommitThreshold: consumerRunOptions.autoCommitThreshold,
  eachBatchAutoResolve: consumerRunOptions.eachBatchAutoResolve,
  partitionsConsumedConcurrently:
    consumerRunOptions.partitionsConsumedConcurrently,
  eachMessage: eachMessageHandler
});

export const getBatchConsumerRunConfig = (
  eachBatchHandler: EachBatchHandler,
  consumerRunOptions: IConsumerRunOptions
): ConsumerRunConfig => ({
  autoCommit: consumerRunOptions.autoCommit,
  autoCommitInterval: consumerRunOptions.autoCommitInterval,
  autoCommitThreshold: consumerRunOptions.autoCommitThreshold,
  eachBatchAutoResolve: consumerRunOptions.eachBatchAutoResolve,
  partitionsConsumedConcurrently:
    consumerRunOptions.partitionsConsumedConcurrently,
  eachBatch: eachBatchHandler
});

export const run = (config: ConsumerRunConfig) => (
  client: Consumer
): TE.TaskEither<Error, void> =>
  TE.tryCatch(() => client.run(config), E.toError);

export enum ReadType {
  Batch,
  Message
}

export const read = (fa: KafkaConsumerCompact) => (
  inputTopic: string,
  readType: ReadType = ReadType.Message,
  handler: (payload: KafkaConsumerPayload) => Promise<void>,
  consumerRunOptions: IConsumerRunOptions = {}
): TE.TaskEither<Error, void> =>
  pipe(
    fa,
    TE.fromIO,
    TE.bindTo("client"),
    TE.mapLeft(E.toError),
    TE.chain(({ client }) =>
      pipe(
        TE.Do,
        TE.bind("consumer", () => TE.of(client.consumer)),
        TE.chainFirst(({ consumer }) => pipe(consumer, connect)),
        TE.chainFirst(({ consumer }) =>
          pipe(
            consumer,
            subscribe({ topics: [inputTopic], fromBeginning: true })
          )
        ),
        TE.chainFirst(({ consumer }) =>
          pipe(
            readType === ReadType.Message
              ? getMessageConsumerRunConfig(handler, consumerRunOptions)
              : getBatchConsumerRunConfig(handler, consumerRunOptions),
            runConfig => pipe(consumer, run(runConfig))
          )
        )
      )
    ),
    TE.bimap(
      error => new Error(`Error reading the message: ${error}`),
      constVoid
    )
  );
