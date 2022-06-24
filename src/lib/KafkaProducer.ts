import * as E from "fp-ts/Either";
import * as IO from "fp-ts/IO";
import { flow, pipe } from "fp-ts/lib/function";
import * as O from "fp-ts/Option";
import * as RA from "fp-ts/ReadonlyArray";
import * as TE from "fp-ts/TaskEither";
import { Kafka, Message, Producer, RecordMetadata } from "kafkajs";
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

const MAX_CHUNK_SIZE = 500;

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

export const publishChunks = <B>(
  client: Producer,
  messages: ReadonlyArray<B>,
  topic: KafkaProducerTopicConfig<B>
): TE.TaskEither<
  ReadonlyArray<IStorableSendFailureError<B>>,
  ReadonlyArray<RecordMetadata>
> =>
  pipe(
    messages,
    RA.chunksOf(MAX_CHUNK_SIZE),
    RA.map(chunks =>
      TE.tryCatch(
        () =>
          client.send({
            ...topic,
            messages: messagify(chunks, topic.messageFormatter)
          }),
        e => storableSendFailureError(e, chunks)
      )
    ),
    RA.sequence(TE.ApplicativeSeq),
    TE.map(RA.flatten)
  );

export const send: <B>(
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
    TE.bindW("results", ({ client }) => publishChunks(client, messages, topic)),
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
