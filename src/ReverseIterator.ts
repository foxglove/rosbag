import { compare, subtract as subTime } from "@foxglove/rostime";

import { BaseIterator } from "./BaseIterator";
import { IteratorConstructorArgs, ChunkReadResult } from "./types";

export class ReverseIterator extends BaseIterator {
  constructor(args: IteratorConstructorArgs) {
    // Sort by largest timestamp first
    super(args, (a, b) => {
      return compare(b.time, a.time);
    });
  }

  protected override async loadNext(): Promise<void> {
    let stamp = this.position;

    // These are all chunks that we can consider for iteration.
    // Only consider chunks with a start before or equal to our position.
    // Chunks starting after our position are not part of reverse iteration
    let candidateChunkInfos = this.chunkInfos.filter((info) => {
      return compare(info.startTime, stamp) <= 0;
    });

    // If we only want specific connections (i.e. topics), then we further filter
    // to only the chunks with those topics
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

    // No chunks contain our stamp, find the next chunk(s) before our stamp
    if (chunkInfos.length === 0) {
      let newStamp = stamp;
      for (const candidateChunk of candidateChunkInfos) {
        // The first chunk we see sets the new stamp
        if (newStamp === stamp) {
          chunkInfos = [candidateChunk];
          newStamp = candidateChunk.endTime;
          continue;
        }

        const compareResult = compare(candidateChunk.endTime, newStamp);

        // If the chunk ends before our new stamp it is ignored since the existing
        // chunks are closer to the stamp.
        if (compareResult < 0) {
          continue;
        }
        // If the chunk end is equal to the new stamp, add to chunk infos
        else if (compareResult === 0) {
          chunkInfos.push(candidateChunk);
        }
        // If the chunk end is after the new stamp it is closer to the stamp.
        // Make that chunk the new stamp and selected chunk.
        else if (compareResult > 0) {
          chunkInfos = [candidateChunk];
          newStamp = candidateChunk.endTime;
        }
      }

      // update the stamp to our chunk start
      stamp = newStamp;
    }

    // End of file or no more candidates
    if (chunkInfos.length === 0) {
      return;
    }

    // Get the earliest start time across all the chunks we've selected
    let start = chunkInfos[0]!.startTime;
    for (const info of chunkInfos) {
      if (compare(info.startTime, start) < 0) {
        start = info.startTime;
      }
    }

    // There might be some candidate chunks which end between our start and stamp.
    // Since we read from start to stamp, we need to include those chunks as well.
    for (const info of candidateChunkInfos) {
      // NOTE: end time is strictly less than stamp because end times equal to stamp
      // have already been considered and we should not include chunks twice.
      if (compare(info.endTime, start) >= 0 && compare(info.endTime, stamp) < 0) {
        chunkInfos.push(info);
      }
    }

    // Subtract 1 nsec to make the next position 1 before
    this.position = start = subTime(start, { sec: 0, nsec: 1 });

    const heap = this.heap;
    const newCache = new Map<number, ChunkReadResult>();
    for (const chunkInfo of chunkInfos) {
      let result = this.cachedChunkReadResults.get(chunkInfo.chunkPosition);
      if (!result) {
        result = await this.reader.readChunk(chunkInfo, this.decompress);
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
}
