// Conditional Exports are not yet widely supported.
// For now we "fake" them by having this top level js file
// https://nodejs.org/api/packages.html#packages_conditional_exports

const { default: FileReader } = require("./dist/cjs/node/FileReader");

module.exports = {
  FileReader,
};
