import * as E from "fp-ts/Either";
import { identity, pipe } from "fp-ts/lib/function";
import * as RA from "fp-ts/ReadonlyArray";
import * as TE from "fp-ts/TaskEither";
import {
  Consumer,
  KafkaJSError,
  KafkaJSProtocolError,
  Producer,
  RecordMetadata
} from "kafkajs";

// eslint-disable-next-line @typescript-eslint/no-var-requires
const { failure, createErrorFromCode } = require("kafkajs/src/protocol/error"); // import required becouse createErrorFromRecord is not included in @types/kafkajs

// eslint-disable-next-line @typescript-eslint/no-var-requires
const kerr = require("kafkajs/src/errors.js"); // due to suspected issue "KafkaJsError is not a costructor" whe using kafkajs type

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
  records: ReadonlyArray<RecordMetadata>
): TE.TaskEither<
  ReadonlyArray<IStorableSendFailureError<T>>,
  ReadonlyArray<RecordMetadata>
> =>
  pipe(
    records,
    RA.filter(r => failure(r.errorCode)),
    RA.mapWithIndex((i, record) => ({
      ...(createErrorFromCode(record.errorCode) as KafkaJSProtocolError), // cast required because createErrorFromRecord is not included in @types/kafkajs
      body: messages[i]
    })),
    TE.fromPredicate(rs => rs.length === 0, identity),
    TE.map(_ => records)
  );

const isKafkaJSError = (error: Error): error is KafkaJSError =>
  "message" in error && "name" in error && "retriable" in error;

export const storableSendFailureError = <T>(
  error: unknown,
  messages: ReadonlyArray<T>
): ReadonlyArray<IStorableSendFailureError<T>> =>
  pipe(
    error,
    E.toError,
    E.fromPredicate(
      isKafkaJSError,
      e => new kerr.KafkaJSError(e, { retriable: false })
    ),
    E.toUnion,
    (ke: KafkaJSError) => messages.map(message => ({ ...ke, body: message }))
  );
