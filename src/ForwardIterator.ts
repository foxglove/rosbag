import { compare, Time, add as addTime } from "@foxglove/rostime";
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

class ForwardIterator {
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

    this.heap = new Heap((a, b) => {
      return compare(a.time, b.time);
    });
  }

  private async loadNext(): Promise<void> {
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

    // Lookup chunks which contain our stamp inclusive of startTime and endTime
    let chunkInfos = candidateChunkInfos.filter((info) => {
      return compare(info.startTime, stamp) <= 0 && compare(stamp, info.endTime) <= 0;
    });

    // No chunks contain our stamp, get the first one after our stamp
    if (chunkInfos.length === 0) {
      type ReducedValue = {
        time?: Time;
        chunkInfos: ChunkInfo[];
      };
      const reducedValue = candidateChunkInfos.reduce<ReducedValue>(
        (prev, info) => {
          const time = prev.time;

          if (compare(stamp, info.startTime) >= 0) {
            return prev;
          }

          if (!time) {
            return {
              time: info.startTime,
              chunkInfos: [info],
            };
          }

          if (compare(time, info.startTime) < 0) {
            return prev;
          } else if (compare(time, info.startTime) === 0) {
            prev.chunkInfos.push(info);
            return prev;
          }

          return {
            time: info.startTime,
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

    // _ T
    // A    [-----]
    // B           [----]

    // _ T
    // A [----]
    // B       [----]

    // `T` is the current timestamp.
    // Here we see some possible chunk ranges.
    // A [------T-----]
    // B      [----------]
    // C              [-----]
    // D          [------]
    // E         [---]
    //
    // A & B include T
    // To determine the maximum time we can iterate to until we need to load more.
    // We take all the earliest end stamp (TE) of the matching chunks (A & B)
    // We find all other chunks where their start time is between T and TE
    // The earliest of these start times is the final TE

    let end = chunkInfos[0]!.endTime;
    for (const info of chunkInfos) {
      if (compare(info.endTime, end) > 0) {
        end = info.endTime;
      }
    }

    // add 1 nsec to make end 1 past the end
    end = addTime(end, { sec: 0, nsec: 1 });

    // There might be chunks that start after our stamp, we set end to the start
    // time of any of those since they are not part of the chunks we are reading.
    for (const info of candidateChunkInfos) {
      if (compare(stamp, info.startTime) < 0 && compare(info.startTime, end) < 0) {
        end = info.startTime;
      }
    }

    this.position = end;

    const heap = this.heap;

    const newCache = new Map<number, ChunkReadResult>();

    for (const chunkInfo of chunkInfos) {
      let result = this.cachedChunkReadResults.get(chunkInfo.chunkPosition);
      if (!result) {
        result = await this.bag.reader.readChunk(chunkInfo, this.decompress);
      }

      // Keep chunk read results for chunks where end is in the chunk
      // End is the next position we will read so we don't need to re-read the chunk
      if (compare(chunkInfo.startTime, end) <= 0 && compare(chunkInfo.endTime, end) >= 0) {
        newCache.set(chunkInfo.chunkPosition, result);
      }

      for (const indexData of result.indices) {
        if (this.connectionIds && !this.connectionIds.has(indexData.conn)) {
          continue;
        }
        for (const indexEntry of indexData.indices ?? []) {
          // ensure: stamp <= entry time < end
          if (compare(indexEntry.time, stamp) < 0 || compare(indexEntry.time, end) >= 0) {
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
        // there are no more items, load more
        if (!this.heap.front()) {
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

export { ForwardIterator };
