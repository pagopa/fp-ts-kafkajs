/* eslint-disable sort-keys */
import * as IO from "fp-ts/IO";
import * as t from "io-ts";
import * as TE from "fp-ts/TaskEither";
import * as E from "fp-ts/Either";
import * as O from "fp-ts/Option";
import { Kafka, Producer, RecordMetadata } from "kafkajs";
import { flow, pipe } from "fp-ts/lib/function";
import { NonEmptyString } from "@pagopa/ts-commons/lib/strings";
import { IStorableSendFailureError } from "./KafkaOperation";
import {
  KafkaProducerTopicConfig,
  MessageFormatter,
  ValidableKafkaProducerConfig
} from "./KafkaTypes";
import { send } from "./KafkaProducer";

export const AzureEventhubSas = t.interface({
  url: NonEmptyString,
  policy: NonEmptyString,
  key: NonEmptyString,
  name: NonEmptyString
});
export type AzureEventhubSas = t.TypeOf<typeof AzureEventhubSas>;

const execAzureEventhubSas = (s: NonEmptyString): RegExpExecArray | null =>
  /Endpoint=sb:\/\/([^/]+)\/;SharedAccessKeyName=([^;]+);SharedAccessKey=([^;]+);EntityPath=(.+)/.exec(
    s
  );
const formatAzureEventhubSas = ({
  url,
  policy,
  key,
  name
}: AzureEventhubSas): string =>
  `Endpoint=sb://${url}/;SharedAccessKeyName=${policy};SharedAccessKey=${key};EntityPath=${name}`;

export const AzureEventhubSasFromString = new t.Type<AzureEventhubSas, string>(
  "AzureEventhubSasFromString",
  AzureEventhubSas.is,
  (i, c) =>
    pipe(
      i,
      NonEmptyString.decode,
      E.chain(
        flow(
          execAzureEventhubSas,
          O.fromNullable,
          E.fromOption(() => "input do not match with expected SAS format"),
          E.orElse(message => t.failure(i, c, message)),
          E.chain(groups =>
            AzureEventhubSas.decode({
              url: groups[1],
              policy: groups[2],
              key: groups[3],
              name: groups[4]
            })
          )
        )
      )
    ),
  formatAzureEventhubSas
);
export type AzureEventhubSasFromString = t.TypeOf<
  typeof AzureEventhubSasFromString
>;

export interface IKafkaProducerCompact<T> {
  readonly producer: Producer;
  readonly topic: KafkaProducerTopicConfig<T>;
}

/**
 * @category: model
 * @since: 1.0.0
 */
export type KafkaProducerCompact<T> = IO.IO<IKafkaProducerCompact<T>>;

/**
 * @param config input kafka brokers and target topic configuration
 * @category: constructor
 * @since: 1.0.0
 */
export const fromConfig = <T>(
  config: ValidableKafkaProducerConfig,
  topic: KafkaProducerTopicConfig<T>
): KafkaProducerCompact<T> => (): IKafkaProducerCompact<T> => ({
  producer: new Kafka(config).producer(config),
  topic
});

export const fromSas = <T>(
  sas: AzureEventhubSas,
  messageFormatter?: MessageFormatter<T>
): KafkaProducerCompact<T> =>
  pipe(
    {
      brokers: [`${sas.url}:9093`],
      ssl: true,
      sasl: {
        mechanism: "plain" as const,
        username: "$ConnectionString",
        password: AzureEventhubSasFromString.encode(sas)
      },
      clientId: sas.policy,
      idempotent: true,
      transactionalId: sas.policy,
      maxInFlightRequests: 1,
      messageFormatter,
      topic: sas.name
    },
    fullConfig => fromConfig(fullConfig, fullConfig)
  );

export const sendMessages: <T>(
  fa: KafkaProducerCompact<T>
) => (
  messages: ReadonlyArray<T>
) => TE.TaskEither<
  ReadonlyArray<IStorableSendFailureError<T>>,
  ReadonlyArray<RecordMetadata>
> = fa => flow(messages => send(fa().topic, messages, () => fa().producer));
