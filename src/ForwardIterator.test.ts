import { ForwardIterator } from "./ForwardIterator";
import { consumeMessages, FakeBagReader, generateFixtures } from "./test_support/iterator";

describe("ForwardIterator", () => {
  it("should iterate empty bag", async () => {
    const iterator = new ForwardIterator({
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

    const iterator = new ForwardIterator({
      connections,
      chunkInfos,
      decompress: {},
      reader,
      position: { sec: 0, nsec: 0 },
    });

    const actualMessages = await consumeMessages(iterator);
    expect(actualMessages).toEqual(expectedMessages);
  });

  it("should ignore messages before position", async () => {
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

    const iterator = new ForwardIterator({
      connections,
      chunkInfos,
      decompress: {},
      reader,
      position: { sec: 0, nsec: 1 },
    });

    const actualMessages = await consumeMessages(iterator);
    expect(actualMessages).toEqual(expectedMessages.filter((msg) => msg.timestamp.nsec >= 1));
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

    const iterator = new ForwardIterator({
      connections,
      chunkInfos,
      decompress: {},
      reader,
      position: { sec: 0, nsec: 0 },
    });

    const actualMessages = await consumeMessages(iterator);
    expect(actualMessages).toEqual(expectedMessages);
  });

  // Test when the there is a chunk with messages before and after the position BUT
  // The messages we want are before the position.
  //
  // [AABB][AABB]
  //     ^
  it("should iterate when messages are before position and after", async () => {
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
            {
              connection: 1,
              time: 2,
              value: 3,
            },
            {
              connection: 1,
              time: 3,
              value: 4,
            },
          ],
        },
        {
          messages: [
            {
              connection: 0,
              time: 4,
              value: 5,
            },
          ],
        },
      ],
    });

    const iterator = new ForwardIterator({
      connections,
      chunkInfos,
      decompress: {},
      reader,
      position: { sec: 0, nsec: 3 },
      topics: ["/0"],
    });

    const actualMessages = await consumeMessages(iterator);
    expect(actualMessages).toEqual(expectedMessages.filter((msg) => msg.timestamp.nsec >= 4));
  });
});
