import { Time } from "@foxglove/rostime";

import { ForwardIterator } from "./ForwardIterator";
import { IBagReader } from "./IBagReader";
import { ChunkInfo, Connection, IndexData, MessageData, Record } from "./record";
import { ChunkReadResult, Constructor } from "./types";

type EnhancedChunkInfo = ChunkInfo & {
  messages: FixtureMessage[];
};

type FixtureMessage = {
  connection: number;
  time: number;
  value: number;
};

type FixtureInput = {
  chunks: {
    messages: FixtureMessage[];
  }[];
};

type FixtureOutput = {
  connections: Map<number, Connection>;
  chunkInfos: ChunkInfo[];
  reader: IBagReader;
  expectedMessages: { conn: number; time: Time; value: number }[];
};

class FakeBagReader implements IBagReader {
  private chunks: EnhancedChunkInfo[];

  constructor(chunks: EnhancedChunkInfo[]) {
    this.chunks = chunks;
  }

  async readChunk(chunkInfo: ChunkInfo): Promise<ChunkReadResult> {
    const chunk = this.chunks[chunkInfo.chunkPosition];
    if (!chunk) {
      throw new Error(`No chunk for position ${chunkInfo.chunkPosition}`);
    }

    const indexByConnId = new Map<number, IndexData>();

    const offsets = [];
    let offset = 0;
    for (const msg of chunk.messages) {
      let indexData = indexByConnId.get(msg.connection);
      if (!indexData) {
        indexData = {
          ver: 1,
          conn: msg.connection,
          count: 1,
          indices: [],
          parseData: () => {},
        };
        indexByConnId.set(msg.connection, indexData);
      }

      indexData.indices?.push({
        time: {
          sec: 0,
          nsec: msg.time,
        },
        offset,
      });

      offsets.push(offset);
      offset += 1;
    }

    return {
      chunk: {
        compression: "",
        size: chunk.messages.length,
        data: new Uint8Array(offsets),
        dataOffset: chunkInfo.chunkPosition,
        parseData: () => {},
      },
      indices: Array.from(indexByConnId.values()),
    };
  }

  readRecordFromBuffer<T extends Record>(
    buffer: Uint8Array,
    fileOffset: number,
    cls: Constructor<T> & { opcode: number },
  ): T {
    const chunk = this.chunks[fileOffset];

    const message = chunk?.messages[buffer[0]!];

    const out = new cls({
      conn: new Uint8Array([0, 0, 0, message!.connection]),
      time: new Uint8Array([0, 0, 0, 0, message!.time, 0, 0, 0]),
    });

    // patch value onto our output to compare with expected data
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    (out as any).value = message?.value;

    return out;
  }
}

function generateFixtures(input: FixtureInput): FixtureOutput {
  const connections: FixtureOutput["connections"] = new Map();
  const chunkInfos: EnhancedChunkInfo[] = [];
  const expectedMessages: FixtureMessage[] = [];

  for (const chunk of input.chunks) {
    let startNs = Infinity;
    let endNs = 0;
    for (const msg of chunk.messages) {
      startNs = Math.min(msg.time, startNs);
      endNs = Math.max(msg.time, endNs);

      const connId = msg.connection;
      connections.set(connId, {
        conn: connId,
        topic: `/${connId}`,
        messageDefinition: `${connId}`,
        parseData: () => {},
      });

      expectedMessages.push(msg);
    }

    const idx = chunkInfos.length;
    chunkInfos.push({
      ver: 1,
      chunkPosition: idx,
      startTime: { sec: 0, nsec: startNs },
      endTime: { sec: 0, nsec: endNs },
      count: 1,
      connections: [],
      parseData: () => {},
      messages: chunk.messages,
    });
  }

  expectedMessages.sort((a, b) => a.time - b.time);

  const reader = new FakeBagReader(chunkInfos);
  return {
    connections,
    chunkInfos,
    reader,
    expectedMessages: expectedMessages.map((msg) => {
      return {
        conn: msg.connection,
        time: { sec: 0, nsec: msg.time },
        value: msg.value,
      };
    }),
  };
}

async function consumeMessages(iterator: ForwardIterator): Promise<MessageData[]> {
  const data = [];
  for await (const msg of iterator) {
    data.push(msg);
  }
  return data;
}

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
    expect(actualMessages).toEqual(expectedMessages.filter((msg) => msg.time.nsec >= 1));
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
});
