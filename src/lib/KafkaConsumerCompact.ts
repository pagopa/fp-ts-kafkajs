/* eslint-disable sort-keys */
import * as E from "fp-ts/Either";
import * as IO from "fp-ts/IO";
import * as O from "fp-ts/Option";
import * as TE from "fp-ts/TaskEither";
import * as B from "fp-ts/boolean";
import { constVoid, pipe } from "fp-ts/function";

import {
  Consumer,
  ConsumerRunConfig,
  ConsumerSubscribeTopics,
  EachBatchHandler,
  Kafka
} from "kafkajs";
import { connect } from "./KafkaOperation";
import {
  AzureEventhubSas,
  AzureEventhubSasFromString
} from "./KafkaProducerCompact";
import { ValidableKafkaConsumerConfig } from "./KafkaTypes";

export enum ReadType {
  Batch,
  Message
}

export interface IKafkaConsumerCompact {
  readonly consumer: Consumer;
}

export type KafkaConsumerCompact = IO.IO<IKafkaConsumerCompact>;

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

export const getConsumerRunConfig = (
  runnerConfig: RunnerConfig
): ConsumerRunConfig =>
  pipe(
    {
      autoCommit: runnerConfig.autoCommit,
      autoCommitInterval: runnerConfig.autoCommitInterval,
      autoCommitThreshold: runnerConfig.autoCommitThreshold,
      eachBatchAutoResolve: runnerConfig.eachBatchAutoResolve,
      partitionsConsumedConcurrently:
        runnerConfig.partitionsConsumedConcurrently
    },
    commonConfig =>
      pipe(
        runnerConfig.readType === ReadType.Message,
        B.fold(
          () => ({
            ...commonConfig,
            eachBatch: runnerConfig.handler
          }),
          () => ({
            ...commonConfig,
            eachMessage: runnerConfig.handler,
            eachBatch: undefined
          })
        )
      )
  );

export type RunnerConfig =
  | {
      readonly readType: ReadType.Message;
      readonly handler: EachBatchHandler;
      readonly autoCommit: boolean;
      readonly autoCommitInterval: number;
      readonly autoCommitThreshold: number;
      readonly eachBatchAutoResolve: boolean;
      readonly partitionsConsumedConcurrently: number;
    }
  | {
      readonly readType: ReadType.Batch;
      readonly handler: EachBatchHandler;
      readonly autoCommit: boolean;
      readonly autoCommitInterval: number;
      readonly autoCommitThreshold: number;
      readonly eachBatchAutoResolve: boolean;
      readonly partitionsConsumedConcurrently: number;
    };

interface IRunner {
  readonly run: (
    runConfig: ConsumerRunConfig
  ) => (consumer: Consumer) => TE.TaskEither<Error, void>;
}

export const defaultRunner: IRunner = {
  run: (runConfig: ConsumerRunConfig) => (
    consumer: Consumer
  ): TE.TaskEither<Error, void> =>
    TE.tryCatch(() => consumer.run(runConfig), E.toError)
};

export const read = (fa: KafkaConsumerCompact) => (
  subscription: ConsumerSubscribeTopics,
  runner: IRunner,
  runnerConfig: RunnerConfig
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
          pipe(consumer, subscribe(subscription))
        ),
        TE.chainFirst(({ consumer }) =>
          pipe(consumer, runner.run(runnerConfig))
        )
      )
    ),
    TE.bimap(
      error => new Error(`Error reading the message: ${error}`),
      constVoid
    )
  );
