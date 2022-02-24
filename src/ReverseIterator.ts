import { compare, Time, subtract as subTime } from "@foxglove/rostime";
import Heap from "heap";

import type Bag from "./Bag";
import { ChunkReadResult } from "./BagReader";
import { MessageData } from "./record";

class ReverseIterator {
  private bag: Bag;
  private heap: Heap<{ time: Time; offset: number; chunkReadResult: ChunkReadResult }>;
  private currentTimestamp: Time;

  constructor(opt: { timestamp: Time; bag: Bag }) {
    this.currentTimestamp = opt.timestamp;
    this.bag = opt.bag;

    // Sort by largest timestamp first
    this.heap = new Heap((a, b) => {
      return compare(b.time, a.time);
    });
  }

  async loadNext(): Promise<void> {
    const stamp = this.currentTimestamp;

    // Get the chunks that contain our stamp
    const chunkInfos = this.bag.chunkInfos.filter((info) => {
      return compare(info.startTime, stamp) <= 0 && compare(stamp, info.endTime) <= 0;
    });

    if (chunkInfos.length === 0) {
      return;
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
    const terminalChunks = this.bag.chunkInfos.filter((info) => {
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
