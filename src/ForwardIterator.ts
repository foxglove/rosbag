import { compare, Time, add as addTime } from "@foxglove/rostime";
import Heap from "heap";

import type Bag from "./Bag";
import { ChunkReadResult } from "./BagReader";
import { MessageData } from "./record";

class ForwardIterator {
  private bag: Bag;
  private heap: Heap<{ time: Time; offset: number; chunkReadResult: ChunkReadResult }>;
  private currentTimestamp: Time;

  constructor(opt: { timestamp: Time; bag: Bag }) {
    this.currentTimestamp = opt.timestamp;
    this.bag = opt.bag;

    this.heap = new Heap((a, b) => {
      return compare(a.time, b.time);
    });
  }

  async loadNext(): Promise<void> {
    const stamp = this.currentTimestamp;

    const chunkInfos = this.bag.chunkInfos.filter((info) => {
      return compare(info.startTime, stamp) <= 0 && compare(stamp, info.endTime) <= 0;
    });

    if (chunkInfos.length === 0) {
      return;
    }

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

    // Get the earliest end time across all matching chunks
    let end = chunkInfos[0]!.endTime;
    for (const info of chunkInfos) {
      if (compare(info.endTime, end) < 0) {
        end = info.endTime;
      }
    }

    // fixme - we want end to be 1 nanosecond after that?
    // otherwise we are setting the same end
    end = addTime(end, { sec: 0, nsec: 1 });

    // All chunks with start time between stamp and end
    const terminalChunks = this.bag.chunkInfos.filter((info) => {
      return compare(stamp, info.startTime) < 0 && compare(info.startTime, end) < 0;
    });

    // get the earliest of those start times as the new end
    for (const info of terminalChunks) {
      if (compare(info.startTime, end) < 0) {
        end = info.startTime;
      }
    }

    this.currentTimestamp = end;

    const heap = this.heap;
    for (const chunkInfo of chunkInfos) {
      const result = await this.bag.reader.readChunk(chunkInfo, {});

      for (const indexData of result.indices) {
        for (const indexEntry of indexData.indices ?? []) {
          // skip any time that is before our current timestamp or after end, we will never iterate to those
          if (compare(indexEntry.time, stamp) < 0 && compare(indexEntry.time, end) > 0) {
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

export { ForwardIterator };
