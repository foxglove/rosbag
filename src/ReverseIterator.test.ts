import { ReverseIterator } from "./ReverseIterator";
import { consumeMessages, FakeBagReader, generateFixtures } from "./test_support/iterator";

describe("ReverseIterator", () => {
  it("should iterate empty bag", async () => {
    const iterator = new ReverseIterator({
      connections: new Map(),
      chunkInfos: [],
      decompress: {},
      reader: new FakeBagReader([]),
      position: { sec: 0, nsec: 0 },
    });

    const messages = await consumeMessages(iterator);
    expect(messages).toEqual([]);
  });

  it("should iterate through a chunk", async () => {
    const { connections, chunkInfos, reader, expectedMessages } = generateFixtures({
      chunks: [
        {
          messages: [
            {
              connection: 0,
              time: 0,
              value: 1,
            },
            {
              connection: 0,
              time: 0,
              value: 2,
            },
            {
              connection: 0,
              time: 1,
              value: 3,
            },
          ],
        },
      ],
    });

    const iterator = new ReverseIterator({
      connections,
      chunkInfos,
      decompress: {},
      reader,
      position: { sec: 0, nsec: 1 },
    });

    const actualMessages = await consumeMessages(iterator);
    expect(actualMessages).toEqual(expectedMessages.reverse());
  });

  it("should ignore messages after position", async () => {
    const { connections, chunkInfos, reader, expectedMessages } = generateFixtures({
      chunks: [
        {
          messages: [
            {
              connection: 0,
              time: 0,
              value: 1,
            },
            {
              connection: 0,
              time: 1,
              value: 2,
            },
          ],
        },
      ],
    });

    const iterator = new ReverseIterator({
      connections,
      chunkInfos,
      decompress: {},
      reader,
      position: { sec: 0, nsec: 0 },
    });

    const actualMessages = await consumeMessages(iterator);
    expect(actualMessages).toEqual(
      expectedMessages.reverse().filter((msg) => msg.timestamp.nsec <= 0),
    );
  });

  it("should read multiple overlapping chunks", async () => {
    const { connections, chunkInfos, reader, expectedMessages } = generateFixtures({
      chunks: [
        {
          messages: [
            {
              connection: 0,
              time: 0,
              value: 1,
            },
            {
              connection: 0,
              time: 1,
              value: 2,
            },
          ],
        },
        {
          messages: [
            {
              connection: 0,
              time: 0,
              value: 3,
            },
            {
              connection: 0,
              time: 1,
              value: 4,
            },
          ],
        },
      ],
    });

    const iterator = new ReverseIterator({
      connections,
      chunkInfos,
      decompress: {},
      reader,
      position: { sec: 0, nsec: 1 },
    });

    const actualMessages = await consumeMessages(iterator);
    expect(actualMessages).toEqual(
      expectedMessages.sort((a, b) => b.timestamp.nsec - a.timestamp.nsec),
    );
  });

  it("should include chunks which are within other chunk ranges", async () => {
    const { connections, chunkInfos, reader, expectedMessages } = generateFixtures({
      chunks: [
        {
          messages: [
            {
              connection: 0,
              time: 2,
              value: 1,
            },
            {
              connection: 0,
              time: 10,
              value: 2,
            },
          ],
        },
        {
          messages: [
            {
              connection: 0,
              time: 3,
              value: 3,
            },
            {
              connection: 0,
              time: 5,
              value: 4,
            },
          ],
        },
      ],
    });

    const iterator = new ReverseIterator({
      connections,
      chunkInfos,
      decompress: {},
      reader,
      position: { sec: 0, nsec: 12 },
    });

    const actualMessages = await consumeMessages(iterator);
    expect(actualMessages).toEqual(expectedMessages.reverse());
  });
});
