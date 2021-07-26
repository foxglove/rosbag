// Copyright 2018-2020 Cruise LLC
// Copyright 2021 Foxglove Technologies Inc
//
// This source code is licensed under the Apache License, Version 2.0,
// found in the LICENSE file in the root directory of this source tree.
// You may not use this file except in compliance with the License.

export type RawFields = { [k: string]: Uint8Array };

export interface Constructor<T> {
  new (fields: RawFields): T;
}

export interface Filelike {
  read(offset: number, length: number): Promise<Uint8Array>;
  size(): number;
}
