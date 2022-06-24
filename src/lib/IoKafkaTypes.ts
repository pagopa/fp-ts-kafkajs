/* eslint-disable sort-keys */

/** Partially generated with ts-to-io lib */
/** TODO: improve ts-to-io to use type-from-string decoder from ts-commons */

import * as t from "io-ts";
import { CompressionTypes } from "kafkajs";
import { CommaSeparatedListOf } from "@pagopa/ts-commons/lib/comma-separated-list";
import { BooleanFromString } from "@pagopa/ts-commons/lib/booleans";
import { IntegerFromString } from "@pagopa/ts-commons/lib/numbers";

export const ValidableKafkaProducerConfig = t.intersection([
  t.intersection([
    t.type({
      brokers: t.union([
        t.array(t.string),
        t.Function,
        CommaSeparatedListOf(t.string)
      ])
    }),
    t.partial({
      ssl: t.union([t.undefined, BooleanFromString]),
      sasl: t.union([
        t.undefined,
        t.intersection([
          t.type({ mechanism: t.literal("plain") }),
          t.type({ username: t.string, password: t.string })
        ]),
        t.intersection([
          t.type({ mechanism: t.literal("scram-sha-256") }),
          t.type({ username: t.string, password: t.string })
        ]),
        t.intersection([
          t.type({ mechanism: t.literal("scram-sha-512") }),
          t.type({ username: t.string, password: t.string })
        ]),
        t.intersection([
          t.type({ mechanism: t.literal("aws") }),
          t.intersection([
            t.type({
              authorizationIdentity: t.string,
              accessKeyId: t.string,
              secretAccessKey: t.string
            }),
            t.partial({ sessionToken: t.union([t.undefined, t.string]) })
          ])
        ]),
        t.intersection([
          t.type({ mechanism: t.literal("oauthbearer") }),
          t.type({ oauthBearerProvider: t.Function })
        ])
      ]),
      clientId: t.union([t.undefined, t.string]),
      connectionTimeout: t.union([t.undefined, IntegerFromString]),
      authenticationTimeout: t.union([t.undefined, IntegerFromString]),
      reauthenticationThreshold: t.union([t.undefined, IntegerFromString]),
      requestTimeout: t.union([t.undefined, IntegerFromString]),
      enforceRequestTimeout: t.union([t.undefined, BooleanFromString]),
      retry: t.union([
        t.undefined,
        t.partial({
          maxRetryTime: t.union([t.undefined, IntegerFromString]),
          initialRetryTime: t.union([t.undefined, IntegerFromString]),
          factor: t.union([t.undefined, IntegerFromString]),
          multiplier: t.union([t.undefined, IntegerFromString]),
          retries: t.union([t.undefined, IntegerFromString])
        })
      ]),
      socketFactory: t.union([t.undefined, t.Function]),
      logCreator: t.union([t.undefined, t.Function])
    })
  ]),
  t.partial({
    createPartitioner: t.union([t.undefined, t.Function]),
    retry: t.union([
      t.undefined,
      t.partial({
        maxRetryTime: t.union([t.undefined, IntegerFromString]),
        initialRetryTime: t.union([t.undefined, IntegerFromString]),
        factor: t.union([t.undefined, IntegerFromString]),
        multiplier: t.union([t.undefined, IntegerFromString]),
        retries: t.union([t.undefined, IntegerFromString])
      })
    ]),
    metadataMaxAge: t.union([t.undefined, IntegerFromString]),
    allowAutoTopicCreation: t.union([t.undefined, BooleanFromString]),
    idempotent: t.union([t.undefined, BooleanFromString]),
    transactionalId: t.union([t.undefined, t.string]),
    transactionTimeout: t.union([t.undefined, IntegerFromString]),
    maxInFlightRequests: t.union([t.undefined, IntegerFromString])
  })
]);

export const MessageFormatter = t.Function;

export const KafkaProducerTopicConfig = t.intersection([
  t.type({ topic: t.string }),
  t.partial({
    acks: t.union([t.undefined, IntegerFromString]),
    timeout: t.union([t.undefined, IntegerFromString]),
    compression: t.union([
      t.undefined,
      t.literal(CompressionTypes.None),
      t.literal(CompressionTypes.GZIP),
      t.literal(CompressionTypes.Snappy),
      t.literal(CompressionTypes.LZ4),
      t.literal(CompressionTypes.ZSTD)
    ]),
    messageFormatter: t.union([t.undefined, t.Function])
  })
]);

export const KafkaProducerCompactConfig = t.intersection([
  ValidableKafkaProducerConfig,
  KafkaProducerTopicConfig
]);
