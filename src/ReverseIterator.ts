import { compare, Time, subtract as subTime } from "@foxglove/rostime";
import Heap from "heap";

import type Bag from "./Bag";
import { ChunkReadResult, Decompress } from "./BagReader";
import { ChunkInfo, MessageData } from "./record";

type IteratorConstructorArgs = {
  position: Time;
  bag: Bag;
  topics?: string[];
  decompress: Decompress;
};

class ReverseIterator {
  private bag: Bag;
  private connectionIds?: Set<number>;
  private heap: Heap<{ time: Time; offset: number; chunkReadResult: ChunkReadResult }>;
  private position: Time;
  private decompress: Decompress;

  private cachedChunkReadResults = new Map<number, ChunkReadResult>();

  constructor(args: IteratorConstructorArgs) {
    this.position = args.position;
    this.bag = args.bag;
    this.decompress = args.decompress;

    // if we want to filter by topic, make a list of connection ids to allow
    if (args.topics) {
      const topics = args.topics;
      this.connectionIds = new Set();
      for (const [id, connection] of this.bag.connections) {
        if (topics.includes(connection.topic)) {
          this.connectionIds.add(id);
        }
      }
    }

    // Sort by largest timestamp first
    this.heap = new Heap((a, b) => {
      return compare(b.time, a.time);
    });
  }

  async loadNext(): Promise<void> {
    let stamp = this.position;

    // These are all chunks that contain our connections
    let candidateChunkInfos = this.bag.chunkInfos;

    const connectionIds = this.connectionIds;
    if (connectionIds) {
      candidateChunkInfos = candidateChunkInfos.filter((info) => {
        return info.connections.find((conn) => {
          return connectionIds.has(conn.conn);
        });
      });
    }

    if (candidateChunkInfos.length === 0) {
      return;
    }

    // Get the chunks that contain our stamp
    let chunkInfos = candidateChunkInfos.filter((info) => {
      // If we are filtering on connection ids, only include the chunks that have our connection
      return compare(info.startTime, stamp) <= 0 && compare(stamp, info.endTime) <= 0;
    });

    if (chunkInfos.length === 0) {
      // There are no chunks that contain our stamp
      // Get the oldest chunk right before our stamp

      type ReducedValue = {
        time?: Time;
        chunkInfos: ChunkInfo[];
      };
      const reducedValue = candidateChunkInfos.reduce<ReducedValue>(
        (prev, info) => {
          // exclude any chunks that end after stamp
          if (compare(info.endTime, stamp) >= 0) {
            return prev;
          }

          const time = prev.time;
          if (!time) {
            return {
              time: info.endTime,
              chunkInfos: [info],
            };
          }

          if (compare(time, info.endTime) > 0) {
            return prev;
          } else if (compare(time, info.endTime) === 0) {
            prev.chunkInfos.push(info);
            return prev;
          }

          return {
            time: info.endTime,
            chunkInfos: [info],
          };
        },
        { time: undefined, chunkInfos: [] },
      );

      if (!reducedValue.time) {
        return;
      }

      // update the stamp to our chunk start
      stamp = reducedValue.time;
      chunkInfos = reducedValue.chunkInfos;
    }

    // `T` is the current timestamp.
    // Here we see some possible chunk ranges.
    // A          [------T-----]
    // B        [----------]
    // C      [-----]
    // D [------]
    // E           [---]
    //
    // A & B include T

    // Get the latest start time across all matching chunks
    let start = chunkInfos[0]!.startTime;
    for (const info of chunkInfos) {
      if (compare(info.startTime, start) > 0) {
        start = info.startTime;
      }
    }

    start = subTime(start, { sec: 0, nsec: 1 });

    for (const info of candidateChunkInfos) {
      if (compare(start, info.endTime) < 0 && compare(info.endTime, stamp) < 0) {
        start = info.endTime;
      }
    }

    this.position = start;

    const heap = this.heap;
    const newCache = new Map<number, ChunkReadResult>();

    for (const chunkInfo of chunkInfos) {
      let result = this.cachedChunkReadResults.get(chunkInfo.chunkPosition);
      if (!result) {
        result = await this.bag.reader.readChunk(chunkInfo, this.decompress);
      }

      // Keep chunk read results for chunks where end is in the chunk
      // End is the next position we will read so we don't need to re-read the chunk
      if (compare(chunkInfo.startTime, start) <= 0 && compare(chunkInfo.endTime, start) >= 0) {
        newCache.set(chunkInfo.chunkPosition, result);
      }

      for (const indexData of result.indices) {
        if (this.connectionIds && !this.connectionIds.has(indexData.conn)) {
          continue;
        }
        for (const indexEntry of indexData.indices ?? []) {
          // skip any time that is before our current timestamp or after end, we will never iterate to those
          if (compare(indexEntry.time, start) <= 0 || compare(indexEntry.time, stamp) > 0) {
            continue;
          }
          heap.push({ time: indexEntry.time, offset: indexEntry.offset, chunkReadResult: result });
        }
      }
    }

    this.cachedChunkReadResults = newCache;
  }

  [Symbol.asyncIterator](): AsyncIterator<MessageData | undefined> {
    return {
      next: async () => {
        if (!this.heap.front()) {
          // there are no more items, load more
          await this.loadNext();
        }

        const item = this.heap.pop();
        if (!item) {
          return { done: true, value: undefined };
        }

        const chunk = item.chunkReadResult.chunk;
        const read = this.bag.reader.readRecordFromBuffer(
          chunk.data!.subarray(item.offset),
          chunk.dataOffset!,
          MessageData,
        );

        return { done: false, value: read };
      },
    };
  }
}

export { ReverseIterator };
