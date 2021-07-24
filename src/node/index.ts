// Copyright 2018-2020 Cruise LLC
// Copyright 2021 Foxglove Technologies Inc
//
// This source code is licensed under the Apache License, Version 2.0,
// found in the LICENSE file in the root directory of this source tree.
// You may not use this file except in compliance with the License.

/* eslint-disable filenames/match-exported */

import { Buffer } from "buffer";
import * as fs from "fs";

import Bag from "../Bag";
import BagReader from "../BagReader";
import { Callback } from "../types";

// reader using nodejs fs api
export class Reader {
  _filename: string;
  _fd: number | null | undefined;
  _size: number;
  _buffer: Buffer;

  constructor(filename: string) {
    this._filename = filename;
    this._fd = undefined;
    this._size = 0;
    this._buffer = Buffer.allocUnsafe(0);
  }

  // open a file for reading
  _open(cb: (error: Error | null | undefined) => void): void {
    fs.stat(this._filename, (error, stat) => {
      if (error != null) {
        return cb(error);
      }

      return fs.open(this._filename, "r", (err, fd) => {
        if (err != null) {
          return cb(err);
        }

        this._fd = fd;
        this._size = stat.size;
        return cb(null);
      });
    });
  }

  close(cb: (error: Error | null | undefined) => void): void {
    if (this._fd != null) {
      fs.close(this._fd, cb);
    }
  }

  // read length (bytes) starting from offset (bytes)
  // callback(err, buffer)
  read(offset: number, length: number, cb: Callback<Buffer>): void {
    if (this._fd == null) {
      return this._open((err) => {
        return err != null ? cb(err) : this.read(offset, length, cb);
      });
    }
    if (length > this._buffer.byteLength) {
      this._buffer = Buffer.alloc(length);
    }
    return fs.read(this._fd, this._buffer, 0, length, offset, (err, _bytes, buff) => {
      return err != null ? cb(err) : cb(null, buff);
    });
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
