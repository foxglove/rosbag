// Copyright 2018-2020 Cruise LLC
// Copyright 2021 Foxglove Technologies Inc
//
// This source code is licensed under the Apache License, Version 2.0,
// found in the LICENSE file in the root directory of this source tree.
// You may not use this file except in compliance with the License.

import fs from "fs";
import path from "path";

import { Reader } from ".";

describe("node entrypoint", () => {
  describe("Reader", () => {
    const fixture = path.join(__dirname, "..", "..", "fixtures", "asci-file.txt");

    it("should read bytes from a file", async () => {
      const reader = new Reader(fixture);
      const buff = await reader.read(5, 10);
      expect(reader.size()).toBe(fs.statSync(fixture).size);
      expect(buff.toString()).toBe("6789012345");
    });
  });
});
