import type { Time } from "@foxglove/rostime";
import Heap from "heap";

import { IBagReader } from "./IBagReader";
import { ChunkInfo, Connection, MessageData } from "./record";
import type {
  ChunkReadResult,
  IteratorConstructorArgs,
  MessageIterator,
  MessageEvent,
  Decompress,
} from "./types";

type HeapItem = { time: Time; offset: number; chunkReadResult: ChunkReadResult };

export abstract class BaseIterator implements MessageIterator {
  private connections: Map<number, Connection>;
  private parse?: IteratorConstructorArgs["parse"];

  protected connectionIds?: Set<number>;
  protected reader: IBagReader;
  protected heap: Heap<HeapItem>;
  protected position: Time;
  protected decompress: Decompress;
  protected chunkInfos: ChunkInfo[];
  protected cachedChunkReadResults = new Map<number, ChunkReadResult>();

  constructor(args: IteratorConstructorArgs, compare: (a: HeapItem, b: HeapItem) => number) {
    this.connections = args.connections;
    this.reader = args.reader;
    this.position = args.position;
    this.decompress = args.decompress;
    this.reader = args.reader;
    this.chunkInfos = args.chunkInfos;
    this.heap = new Heap(compare);

    // if we want to filter by topic, make a list of connection ids to allow
    if (args.topics) {
      const topics = args.topics;
      this.connectionIds = new Set();
      for (const [id, connection] of args.connections) {
        if (topics.includes(connection.topic)) {
          this.connectionIds.add(id);
        }
      }
    }
  }

  // Load the next set of messages into the heap
  protected abstract loadNext(): Promise<void>;

  /**
   * @returns An AsyncIterator of MessageEvents
   */
  async *[Symbol.asyncIterator](): AsyncIterator<MessageEvent> {
    while (true) {
      if (!this.heap.front()) {
        await this.loadNext();
      }

      const item = this.heap.pop();
      if (!item) {
        return;
      }

      const chunk = item.chunkReadResult.chunk;
      const messageData = this.reader.readRecordFromBuffer(
        chunk.data!.subarray(item.offset),
        chunk.dataOffset!,
        MessageData,
      );

      const connection = this.connections.get(messageData.conn);
      if (!connection) {
        throw new Error(`Unable to find connection with id ${messageData.conn}`);
      }

      const { topic } = connection;
      const { data, time } = messageData;
      if (!data) {
        throw new Error(`No data in message for topic: ${topic}`);
      }

      const event: MessageEvent = {
        topic,
        timestamp: time,
        data,
        message: this.parse?.(data, connection),
      };

      yield event;
    }
  }
}
