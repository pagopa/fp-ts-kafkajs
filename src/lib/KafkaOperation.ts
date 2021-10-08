import * as TE from "fp-ts/TaskEither";
import * as E from "fp-ts/Either";
import * as AR from "fp-ts/Array";
import {
  Consumer,
  KafkaJSError,
  KafkaJSProtocolError,
  Producer,
  RecordMetadata
} from "kafkajs";
import { identity, pipe } from "fp-ts/lib/function";

// eslint-disable-next-line @typescript-eslint/no-var-requires
const { failure, createErrorFromCode } = require("kafkajs/src/protocol/error"); // import required becouse createErrorFromRecord is not included in @types/kafkajs

export interface IStorableSendFailureError<T> extends KafkaJSError {
  readonly body: T;
}

export const connect = (
  client: Producer | Consumer
): TE.TaskEither<Error, void> => TE.tryCatch(client.connect, E.toError);

export const disconnect = (
  client: Producer | Consumer
): TE.TaskEither<Error, void> => TE.tryCatch(client.disconnect, E.toError);

export const disconnectWithoutError = (
  client: Producer | Consumer
): TE.TaskEither<never, void> =>
  pipe(
    client,
    disconnect,
    TE.orElseW(_ => TE.right(undefined))
  );

export const processErrors = <T>(
  messages: ReadonlyArray<T>,
  // eslint-disable-next-line functional/prefer-readonly-type
  records: RecordMetadata[]
): TE.TaskEither<
  ReadonlyArray<IStorableSendFailureError<T>>,
  ReadonlyArray<RecordMetadata>
> =>
  pipe(
    records,
    AR.filter(r => failure(r.errorCode)),
    AR.mapWithIndex((i, record) => ({
      ...(createErrorFromCode(record.errorCode) as KafkaJSProtocolError), // cast required becouse createErrorFromRecord is not included in @types/kafkajs
      body: messages[i]
    })),
    TE.fromPredicate(rs => rs.length === 0, identity),
    TE.map(_ => records)
  );

const isKafkaJSError = (error: Error): error is KafkaJSError =>
  error instanceof KafkaJSError;

export const storableSendFailureError = <T>(
  error: unknown,
  messages: ReadonlyArray<T>
): ReadonlyArray<IStorableSendFailureError<T>> =>
  pipe(
    error,
    E.toError,
    E.fromPredicate(
      isKafkaJSError,
      e => new KafkaJSError(e, { retriable: false })
    ),
    E.toUnion,
    (ke: KafkaJSError) => messages.map(message => ({ ...ke, body: message }))
  );
