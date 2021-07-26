// Copyright 2018-2020 Cruise LLC
// Copyright 2021 Foxglove Technologies Inc
//
// This source code is licensed under the Apache License, Version 2.0,
// found in the LICENSE file in the root directory of this source tree.
// You may not use this file except in compliance with the License.

/* eslint-disable filenames/match-exported */

import { Buffer } from "buffer";
import * as fs from "fs/promises";

import Bag from "../Bag";
import BagReader from "../BagReader";
import { Filelike } from "../types";

// reader using nodejs fs api
export class Reader implements Filelike {
  _filename: string;
  _file?: fs.FileHandle;
  _size: number;
  _buffer: Buffer;

  constructor(filename: string) {
    this._filename = filename;
    this._file = undefined;
    this._size = 0;
    this._buffer = Buffer.allocUnsafe(0);
  }

  // open a file for reading
  private async _open(): Promise<void> {
    this._file = await fs.open(this._filename, "r");
    this._size = (await this._file.stat()).size;
  }

  async close(): Promise<void> {
    await this._file?.close();
  }

  // read length (bytes) starting from offset (bytes)
  // callback(err, buffer)
  async read(offset: number, length: number): Promise<Buffer> {
    if (this._file == null) {
      await this._open();
      return await this.read(offset, length);
    }
    if (length > this._buffer.byteLength) {
      this._buffer = Buffer.alloc(length);
    }
    const { bytesRead } = await this._file.read(this._buffer, 0, length, offset);
    if (bytesRead < length) {
      throw new Error(`Attempted to read ${length} bytes at offset ${offset} but only ${bytesRead} were available`);
    }
    return this._buffer;
  }

  // return the size of the file
  size(): number {
    return this._size;
  }
}

const open = async (filename: File | string): Promise<Bag> => {
  if (typeof filename !== "string") {
    throw new Error(
      "Expected filename to be a string. Make sure you are correctly importing the node or web version of Bag."
    );
  }
  const bag = new Bag(new BagReader(new Reader(filename)));
  await bag.open();
  return bag;
};
Bag.open = open;

export type { Filelike } from "../types";
export { BagReader, open };
export default Bag;
