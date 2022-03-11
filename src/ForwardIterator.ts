import { compare, add as addTime } from "@foxglove/rostime";

import { BaseIterator } from "./BaseIterator";
import { IteratorConstructorArgs, ChunkReadResult } from "./types";

export class ForwardIterator extends BaseIterator {
  constructor(args: IteratorConstructorArgs) {
    // Sort by smallest timestamp first
    super(args, (a, b) => {
      return compare(a.time, b.time);
    });
  }

  protected override async loadNext(): Promise<void> {
    let stamp = this.position;

    // These are all chunks that we can consider for iteration.
    // Only consider chunks with an endTime after or equal to our position.
    // Chunks before our position are not part of forward iteration.
    let candidateChunkInfos = this.chunkInfos.filter((info) => {
      return compare(info.endTime, stamp) >= 0;
    });

    if (candidateChunkInfos.length === 0) {
      return;
    }

    // Lookup chunks which contain our stamp inclusive of startTime and endTime
    let chunkInfos = candidateChunkInfos.filter((info) => {
      return compare(info.startTime, stamp) <= 0 && compare(stamp, info.endTime) <= 0;
    });

    // No chunks contain our stamp, find the next chunk(s) after our stamp
    if (chunkInfos.length === 0) {
      let newStamp = stamp;
      for (const candidateChunk of candidateChunkInfos) {
        // The first chunk we see sets the new stamp
        if (newStamp === stamp) {
          chunkInfos = [candidateChunk];
          newStamp = candidateChunk.startTime;
          continue;
        }

        const compareResult = compare(newStamp, candidateChunk.startTime);

        // If the chunk starts after our new stamp it is ignored since the existing
        // chunks are closer to the stamp.
        if (compareResult < 0) {
          continue;
        }
        // If the chunk start is equal to the new stamp, add to chunk infos
        else if (compareResult === 0) {
          chunkInfos.push(candidateChunk);
        }
        // If the chunk start is before the new stamp it is closer to the stamp.
        // Make that chunk the new stamp and selected chunk.
        else if (compareResult > 0) {
          chunkInfos = [candidateChunk];
          newStamp = candidateChunk.startTime;
        }
      }

      // update the stamp to our chunk start
      stamp = newStamp;
    }

    // End of file or no more candidates
    if (chunkInfos.length === 0) {
      return;
    }

    // The end time is the maximum end time of all the chunks we've selected
    let end = chunkInfos[0]!.endTime;
    for (const info of chunkInfos) {
      if (compare(info.endTime, end) > 0) {
        end = info.endTime;
      }
    }

    // There might be some candidate chunks which start after our stamp and before the end time.
    // Since we plan to read to _end_, we need to include these chunks as well.
    for (const info of candidateChunkInfos) {
      // NOTE: startTime is strictly greater than stamp because start times equal to stamp
      // would have already been considered and we should not include chunks twice.
      if (compare(info.startTime, stamp) > 0 && compare(info.startTime, end) <= 0) {
        chunkInfos.push(info);
      }
    }

    // Add 1 nsec to make end 1 past the end for the next read
    this.position = end = addTime(end, { sec: 0, nsec: 1 });

    const heap = this.heap;
    const newCache = new Map<number, ChunkReadResult>();
    for (const chunkInfo of chunkInfos) {
      let result = this.cachedChunkReadResults.get(chunkInfo.chunkPosition);
      if (!result) {
        result = await this.reader.readChunk(chunkInfo, this.decompress);
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
}
