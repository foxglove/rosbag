import { compare, Time, subtract as subTime } from "@foxglove/rostime";
import Heap from "heap";

import type Bag from "./Bag";
import { ChunkReadResult } from "./BagReader";
import { ChunkInfo, MessageData } from "./record";

class ReverseIterator {
  private bag: Bag;
  private connectionIds?: Set<number>;
  private heap: Heap<{ time: Time; offset: number; chunkReadResult: ChunkReadResult }>;
  private currentTimestamp: Time;

  constructor(opt: { timestamp: Time; bag: Bag; topics?: string[] }) {
    this.currentTimestamp = opt.timestamp;
    this.bag = opt.bag;

    // if we want to filter by topic, make a list of connection ids to allow
    if (opt.topics) {
      const topics = opt.topics;
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
    const stamp = this.currentTimestamp;

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

    // fixme - so no chunk contains our stamp
    // get the oldest end time that is before the stamp

    if (chunkInfos.length === 0) {
      // There are no chunks that contain our stamp
      // Get the oldest chunk right before our stamp
      // fixme - what if two chunks have the same end time
      // we should keep both
      const previous = candidateChunkInfos.reduce((prev: ChunkInfo | undefined, info) => {
        if (!prev) {
          if (compare(info.endTime, stamp) < 0) {
            return info;
          }
          return undefined;
        }

        if (compare(info.endTime, prev.endTime) < 0) {
          return prev;
        }

        return info;
      }, undefined);

      if (!previous) {
        return;
      }

      chunkInfos = [previous];
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

    // fixme - we want end to be 1 nanosecond after that?
    // otherwise we are setting the same end
    start = subTime(start, { sec: 0, nsec: 1 });

    // All chunks with end time between start and stamp
    const terminalChunks = candidateChunkInfos.filter((info) => {
      return compare(start, info.endTime) < 0 && compare(info.endTime, stamp) < 0;
    });

    // get the latest of those end times as the new start
    for (const info of terminalChunks) {
      if (compare(start, info.endTime) < 0) {
        start = info.startTime;
      }
    }

    this.currentTimestamp = start;

    const heap = this.heap;
    for (const chunkInfo of chunkInfos) {
      const result = await this.bag.reader.readChunk(chunkInfo, {});

      for (const indexData of result.indices) {
        if (this.connectionIds && !this.connectionIds.has(indexData.conn)) {
          continue;
        }
        for (const indexEntry of indexData.indices ?? []) {
          // skip any time that is before our current timestamp or after end, we will never iterate to those
          if (compare(indexEntry.time, start) < 0 && compare(indexEntry.time, stamp) > 0) {
            continue;
          }
          heap.push({ time: indexEntry.time, offset: indexEntry.offset, chunkReadResult: result });
        }
      }
    }
  }

  async next(): Promise<MessageData | undefined> {
    if (!this.heap.front()) {
      // there are no more items, load more
      await this.loadNext();
    }

    const item = this.heap.pop();
    if (!item) {
      return undefined;
    }

    const chunk = item.chunkReadResult.chunk;
    const read = this.bag.reader.readRecordFromBuffer(
      chunk.data!.subarray(item.offset),
      chunk.dataOffset!,
      MessageData,
    );

    return read;
  }
}

export { ReverseIterator };
