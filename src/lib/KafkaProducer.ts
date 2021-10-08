import * as IO from "fp-ts/IO";
import * as E from "fp-ts/Either";
import * as TE from "fp-ts/TaskEither";
import * as O from "fp-ts/Option";
import { Kafka, Producer, RecordMetadata, Message } from "kafkajs";
import { flow, pipe } from "fp-ts/lib/function";
import {
  connect,
  disconnectWithoutError,
  IStorableSendFailureError,
  processErrors,
  storableSendFailureError
} from "./KafkaOperation";
import {
  KafkaProducerTopicConfig,
  MessageFormatter,
  ValidableKafkaProducerConfig
} from "./KafkaTypes";

/**
 * @category: model
 * @since: 1.0.0
 */
export type KafkaProducer = IO.IO<Producer>;

const jsonMessageFormatter: MessageFormatter<unknown> = message => ({
  value: JSON.stringify(message)
});

const messagify: <T>(
  messages: ReadonlyArray<T>,
  formatter: MessageFormatter<T> | undefined
) => // eslint-disable-next-line functional/prefer-readonly-type
Message[] = (messages, formatter) =>
  pipe(
    formatter,
    O.fromNullable,
    O.getOrElseW(() => jsonMessageFormatter),
    f => IO.of(messages.map(m => f(m)))
  )(); // TODO: remove IO execution and refactor 'send' method to pipe 'messagify' before bindW("results"...)

/**
 * @param config input kafka brokers and target topic configuration
 * @category: constructor
 * @since: 1.0.0
 */
export const fromConfig = (
  config: ValidableKafkaProducerConfig
): KafkaProducer => (): Producer => new Kafka(config).producer(config);

const send: <B>(
  topic: KafkaProducerTopicConfig<B>,
  messages: ReadonlyArray<B>,
  fa: KafkaProducer
) => TE.TaskEither<
  ReadonlyArray<IStorableSendFailureError<B>>,
  ReadonlyArray<RecordMetadata>
> = (
  topic,
  messages,
  fa
  // eslint-disable-next-line @typescript-eslint/explicit-function-return-type
) =>
  pipe(
    fa,
    TE.fromIO,
    TE.mapLeft(E.toError),
    TE.bindTo("client"),
    TE.chainFirst(({ client }) => connect(client)),
    TE.mapLeft(e => storableSendFailureError(e, messages)),
    TE.bindW("results", ({ client }) =>
      TE.tryCatch(
        () =>
          client.send({
            ...topic,
            messages: messagify(messages, topic.messageFormatter)
          }),
        e => storableSendFailureError(e, messages)
      )
    ),
    TE.chainFirstW(({ results }) => processErrors(messages, results)),
    TE.chainFirstW(({ client }) => disconnectWithoutError(client)),
    TE.map(({ results }) => results)
  );

export const sendWithProducer: <B>(
  topic: KafkaProducerTopicConfig<B>,
  messages: ReadonlyArray<B>
) => (
  fa: KafkaProducer
) => TE.TaskEither<
  ReadonlyArray<IStorableSendFailureError<B>>,
  ReadonlyArray<RecordMetadata>
> = (topic, messages) => flow(fa => send(topic, messages, fa));

export const sendMessages: <B>(
  topic: KafkaProducerTopicConfig<B>,
  fa: KafkaProducer
) => (
  messages: ReadonlyArray<B>
) => TE.TaskEither<
  ReadonlyArray<IStorableSendFailureError<B>>,
  ReadonlyArray<RecordMetadata>
> = (topic, fa) => flow(messages => send(topic, messages, fa));
