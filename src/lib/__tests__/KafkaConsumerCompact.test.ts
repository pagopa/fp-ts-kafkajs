/* eslint-disable no-var */
import * as E from "fp-ts/Either";
import * as TE from "fp-ts/TaskEither";
import {
  Consumer,
  ConsumerConfig,
  ConsumerRunConfig,
  KafkaConfig
} from "kafkajs";
import * as EventHubUtils from "../KafkaConsumerCompact";
import {
  ReadType,
  getConsumerFromConfig,
  getConsumerFromSas,
  read,
  subscribe
} from "../KafkaConsumerCompact";
import * as KafkaOperation from "../KafkaOperation";
import { AzureEventhubSasFromString } from "../KafkaProducerCompact";
var mockConsumerFn = jest.fn();
var mockSubscribeFn = jest.fn();
var mockRun = jest.fn();
var mockConnect = jest.fn();
const mockConsumer = ({
  connect: mockConnect,
  run: mockRun,
  subscribe: mockSubscribeFn
} as unknown) as Consumer;

var requireActual = jest.requireActual("kafkajs");
jest.mock("kafkajs", () => ({
  ...requireActual,
  Kafka: jest.fn(() => ({
    consumer: mockConsumerFn
  }))
}));

describe("getConsumerFromConfig", () => {
  it("should create a KafkaConsumerCompact from valid config", () => {
    const validConfig: KafkaConfig & ConsumerConfig = {
      brokers: ["localhost:9092"],
      groupId: "test-group"
    };

    mockConsumerFn.mockImplementationOnce(() => mockConsumer);

    const result = getConsumerFromConfig(validConfig)();

    expect(result.consumer).toBeDefined();
    expect(result.consumer).toBe(mockConsumer);
  });
});

const DUMMY_SAS = {
  key: "dummykeytp5bIGW+QCTtGh8RIpcOCHg2CfJU7ij1uQmA=",
  name: "dummy-name",
  policy: "dummy-policy",
  url: "dummy.servicebus.windows.net"
};
const DUMMY_CONNECTION_STRING = `Endpoint=sb://${DUMMY_SAS.url}/;SharedAccessKeyName=${DUMMY_SAS.policy};SharedAccessKey=${DUMMY_SAS.key};EntityPath=${DUMMY_SAS.name}`;

describe("getConsumerFromSas", () => {
  it("should create a KafkaConsumerCompact from SAS configuration", () => {
    const decoded = AzureEventhubSasFromString.decode(DUMMY_CONNECTION_STRING);

    mockConsumerFn.mockImplementationOnce(() => mockConsumer);
    if (E.isRight(decoded)) {
      const sas = decoded.right;
      const result = getConsumerFromSas(sas)();

      expect(result.consumer).toBeDefined();
      expect(result.consumer).toBe(mockConsumer);
    }
  });
});

const subscriptionOptions = { fromBeginning: true, topics: ["topic"] };
describe("subscribe", () => {
  it("should subscribe to topics successfully", async () => {
    mockSubscribeFn.mockResolvedValueOnce(void 0);
    const result = await subscribe(subscriptionOptions)(mockConsumer)();

    expect(mockConsumer.subscribe).toHaveBeenCalledWith(subscriptionOptions);
    expect(result).toEqual(E.right(void 0));
  });

  it("should subscribe to topics with error", async () => {
    mockSubscribeFn.mockRejectedValueOnce(new Error("error"));
    const result = await subscribe(subscriptionOptions)(mockConsumer)();

    expect(mockConsumer.subscribe).toHaveBeenCalledWith(subscriptionOptions);
    expect(result).toEqual(E.left(new Error("error")));
  });
});

const mockEachMessageHandler = jest.fn();
const mockEachBatchHandler = jest.fn();

const mockConsumerRunOptions = ({
  autoCommit: true,
  autoCommitInterval: 1,
  autoCommitThreshold: 2,
  eachBatchAutoResolve: false,
  partitionsConsumedConcurrently: 4
} as unknown) as ConsumerRunConfig;

const messageRunnerConfig = ({
  readType: ReadType.Message,
  autoCommit: true,
  autoCommitInterval: 1,
  autoCommitThreshold: 2,
  eachBatchAutoResolve: false,
  partitionsConsumedConcurrently: 4,
  handler: mockEachMessageHandler
} as unknown) as EventHubUtils.RunnerConfig;

const batchRunnerConfig = ({
  readType: ReadType.Batch,
  autoCommit: true,
  autoCommitInterval: 1,
  autoCommitThreshold: 2,
  eachBatchAutoResolve: false,
  partitionsConsumedConcurrently: 4,
  handler: mockEachBatchHandler
} as unknown) as EventHubUtils.RunnerConfig;

describe("getConsumerRunConfig", () => {
  it("should create a ConsumerRunConfig for EachMessageHandler", () => {
    const result = EventHubUtils.getConsumerRunConfig(messageRunnerConfig);
    expect(result.eachMessage).toBe(mockEachMessageHandler);
    expect(result.eachBatch).toBe(undefined);
    expect(result.autoCommit).toBe(true);
    expect(result.autoCommitInterval).toBe(1);
    expect(result.autoCommitThreshold).toBe(2);
    expect(result.eachBatchAutoResolve).toBe(false);
    expect(result.partitionsConsumedConcurrently).toBe(4);
  });
});

describe("getBatchConsumerRunConfig", () => {
  it("should create a ConsumerRunConfig for EachBatchHandler", () => {
    const result = EventHubUtils.getConsumerRunConfig(batchRunnerConfig);
    expect(result.eachBatch).toBe(mockEachBatchHandler);
    expect(result.eachMessage).toBe(undefined);
    expect(result.autoCommit).toBe(true);
    expect(result.autoCommitInterval).toBe(1);
    expect(result.autoCommitThreshold).toBe(2);
    expect(result.eachBatchAutoResolve).toBe(false);
    expect(result.partitionsConsumedConcurrently).toBe(4);
  });
});

const consumerMock = ({
  connect: jest.fn(async () => void 0),
  disconnect: jest.fn(async () => void 0)
} as unknown) as Consumer;
// eslint-disable-next-line @typescript-eslint/explicit-function-return-type
const kafkaConsumerMock = () => ({
  consumer: consumerMock
});

const connectSpy = jest.spyOn(KafkaOperation, "connect");
const disconnectWithoutErrorSpy = jest.spyOn(
  KafkaOperation,
  "disconnectWithoutError"
);
const subscribeSpy = jest.spyOn(EventHubUtils, "subscribe");
const runSpy = jest.spyOn(EventHubUtils.defaultRunner, "run");

describe("readMessage", () => {
  const topic = "your-topic";

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("should successfully read a message", async () => {
    connectSpy.mockReturnValueOnce(TE.right(undefined));
    subscribeSpy.mockReturnValueOnce(() => TE.right(undefined));
    runSpy.mockReturnValueOnce(() => TE.right(undefined));
    disconnectWithoutErrorSpy.mockReturnValueOnce(TE.right(undefined));

    const result = await read(kafkaConsumerMock)(
      { topics: [topic] },
      EventHubUtils.defaultRunner,
      messageRunnerConfig
    )();

    expect(connectSpy).toHaveBeenCalledWith(consumerMock);
    expect(subscribeSpy).toHaveBeenCalledWith({
      topics: [topic]
    });

    expect(runSpy).toHaveBeenCalled();
    expect(result).toEqual(E.right(undefined));
  });

  it("should handle error while reading a message", async () => {
    connectSpy.mockReturnValueOnce(TE.right(undefined));
    subscribeSpy.mockReturnValueOnce(() => TE.right(undefined));
    runSpy.mockReturnValueOnce(() => TE.left(new Error("Error")));

    const result = await read(kafkaConsumerMock)(
      { topics: [topic] },
      EventHubUtils.defaultRunner,
      messageRunnerConfig
    )();

    expect(connectSpy).toHaveBeenCalledWith(consumerMock);
    expect(subscribeSpy).toHaveBeenCalledWith({
      topics: [topic]
    });

    expect(runSpy).toHaveBeenCalled();
    expect(result).toEqual(
      E.left(new Error(`Error reading the message: Error: Error`))
    );
  });
});
