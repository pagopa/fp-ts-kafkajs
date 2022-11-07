import { pipe } from "fp-ts/lib/function";
import {
  AzureEventhubSas,
  AzureEventhubSasFromString,
  fromSas
} from "../KafkaProducerCompact";
import * as E from "fp-ts/Either";
import { MessageFormatter } from "../KafkaTypes";
import * as RA from "fp-ts/ReadonlyArray";
import { Producer, ProducerRecord, RecordMetadata } from "kafkajs";
import * as KP from "../KafkaProducerCompact";

const DUMMY_SAS = {
  url: "dummy.servicebus.windows.net",
  key: "dummykeytp5bIGW+QCTtGh8RIpcOCHg2CfJU7ij1uQmA=",
  policy: "dummy-policy",
  name: "dummy-name"
};
const DUMMY_CONNECTION_STRING = `Endpoint=sb://${DUMMY_SAS.url}/;SharedAccessKeyName=${DUMMY_SAS.policy};SharedAccessKey=${DUMMY_SAS.key};EntityPath=${DUMMY_SAS.name}`;

describe("AzureEventhubSasFromString", () => {
  it("GIVEN a valid Eventhub connection string, WHEN the string is decoded, THEN return an right either containing the event hub sas object", () => {
    // GIVEN
    const cs = DUMMY_CONNECTION_STRING;
    // WHEN
    const decoded = AzureEventhubSasFromString.decode(cs);
    //THEN
    expect(decoded).toEqual({
      _tag: "Right",
      right: DUMMY_SAS
    });
  });

  it.each([
    `Endpoint=sb:///;SharedAccessKeyName=${DUMMY_SAS.policy};SharedAccessKey=${DUMMY_SAS.key};EntityPath=${DUMMY_SAS.name}`, //missing url
    `Endpoint=sb://${DUMMY_SAS.url}/;SharedAccessKeyName=;SharedAccessKey=${DUMMY_SAS.key};EntityPath=${DUMMY_SAS.name}`, //missing policy
    `Endpoint=sb://${DUMMY_SAS.url}/;SharedAccessKeyName=${DUMMY_SAS.policy};SharedAccessKey=;EntityPath=${DUMMY_SAS.name}`, //missing key
    `Endpoint=sb://${DUMMY_SAS.url}/;SharedAccessKeyName=${DUMMY_SAS.policy};SharedAccessKey=${DUMMY_SAS.key};EntityPath=`, //missing name
    "DefaultEndpointsProtocol=https;AccountName=dummy;AccountKey=key;EndpointSuffix=core.windows.net" //wrong format
  ])(
    "GIVEN the not valid connection string %s, WHEN the string is decoded, THEN return a left either",
    not_valid_connection_string => {
      // GIVEN
      const cs = not_valid_connection_string;
      // WHEN
      const decoded = AzureEventhubSasFromString.decode(cs);
      // THEN
      expect(decoded).toEqual({
        _tag: "Left",
        left: [
          expect.objectContaining({
            message: "input do not match with expected SAS format",
            value: cs
          })
        ]
      });
    }
  );
});

const formatterMock: MessageFormatter<AzureEventhubSas> = jest.fn(m => ({
  value: JSON.stringify(m)
}));

describe("fromSas", () => {
  it.each([
    { inputSas: DUMMY_SAS, formatter: undefined }, // valid sas without formatter
    { inputSas: DUMMY_SAS, formatter: formatterMock } // valid sas with dummy formatter
  ])(
    "GIVEN the valid Eventhub SAS %p, WHEN the sas is used to configure a kafka producer, THEN create a valid producer with the proper configuration",
    ({ inputSas, formatter }) => {
      // GIVEN
      const sas = pipe(
        inputSas,
        AzureEventhubSas.decode,
        E.getOrElseW(() =>
          fail("You are using a not valid Eventhub SAS for test")
        )
      );
      // WHEN
      const producer = fromSas(sas, formatter)();
      // THEN
      expect(producer).toEqual(
        expect.objectContaining({
          topic: {
            brokers: [`${sas.url}:9093`],
            clientId: sas.policy,
            idempotent: true,
            maxInFlightRequests: 1,
            messageFormatter: formatter,
            sasl: {
              mechanism: "plain",
              password: DUMMY_CONNECTION_STRING,
              username: "$ConnectionString"
            },
            ssl: true,
            topic: sas.name,
            transactionalId: sas.policy
          }
        })
      );
    }
  );
});

const aTopic = "a-topic";
const aDocument = { name: "a-name" };
const anError = new Error("An error");
const aKafkaResponse = {
  errorCode: 0,
  partition: 1,
  topicName: aTopic
};

const mockSendMessage = jest.fn(async (pr: ProducerRecord) =>
  pipe(
    pr.messages,
    RA.map(() => aKafkaResponse)
  )
);
const producerMock = () => ({
  producer: ({
    connect: jest.fn(async () => void 0),
    disconnect: jest.fn(async () => void 0),
    send: mockSendMessage
  } as unknown) as Producer,
  topic: { topic: aTopic }
});

describe("sendMessages", () => {
  it("GIVEN a valid Kafka producer, WHEN publishing a document, THEN publish it to the topic and return a right either", async () => {
    // Given
    const producer = producerMock;
    // When
    const publishedOrError = await pipe(
      [aDocument],
      KP.sendMessages(producer)
    )();
    // Then
    expect(E.isRight(publishedOrError)).toBeTruthy();
    expect(mockSendMessage).toHaveBeenCalledWith({
      messages: [{ value: JSON.stringify(aDocument) }],
      topic: aTopic
    });
  });

  it("GIVEN a not working Kafka producer, WHEN publishing a document, THEN return a left containing a KafkaJSError", async () => {
    // Given
    mockSendMessage.mockImplementationOnce(async () => {
      throw anError;
    });
    const producer = producerMock;
    // When
    const publishedOrError = await pipe(
      [aDocument],
      KP.sendMessages(producer)
    )();
    // Then
    expect(publishedOrError).toEqual({
      _tag: "Left",
      left: [
        expect.objectContaining({
          body: aDocument,
          name: "KafkaJSError",
          retriable: false
        })
      ]
    });
    expect(mockSendMessage).toHaveBeenCalledWith({
      messages: [{ value: JSON.stringify(aDocument) }],
      topic: aTopic
    });
  });

  it("GIVEN a Kafka producer returning an error code 1, WHEN publishing a document, THEN return a left containing a KafkaJSProtocolError", async () => {
    // Given
    mockSendMessage.mockImplementationOnce(async (pr: ProducerRecord) =>
      pipe(
        pr.messages,
        RA.map(() => ({ ...aKafkaResponse, errorCode: 1 }))
      )
    );
    const producer = producerMock;
    // When
    const publishedOrError = await pipe(
      [aDocument],
      KP.sendMessages(producer)
    )();
    // Then
    expect(publishedOrError).toEqual({
      _tag: "Left",
      left: [
        expect.objectContaining({
          body: aDocument,
          name: "KafkaJSProtocolError",
          type: "OFFSET_OUT_OF_RANGE"
        })
      ]
    });
    expect(mockSendMessage).toHaveBeenCalledWith({
      messages: [{ value: JSON.stringify(aDocument) }],
      topic: aTopic
    });
  });
});
