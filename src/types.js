// Copyright (c) 2018-present, GM Cruise LLC

// This source code is licensed under the Apache License, Version 2.0,
// found in the LICENSE file in the root directory of this source tree.
// You may not use this file except in compliance with the License.

// @flow

// A function that must be called with either an error or a value, but not both
export type Callback<T> = ((error: Error, value?: void) => void) & ((error: null, value: T) => void);
