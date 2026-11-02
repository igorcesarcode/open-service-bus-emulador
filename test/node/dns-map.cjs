// Preloaded by TestNodeSDK when OSB_NO_EMULATOR_FLAG is set: resolves the
// namespace name of the real service to the machine running the emulator.
const dns = require("node:dns");
const host = process.env.OSB_MAPPED_HOST;
const lookup = dns.lookup;
dns.lookup = function (name, options, callback) {
  if (typeof options === "function") {
    callback = options;
    options = {};
  }
  if (name === host) {
    return lookup("127.0.0.1", options, callback);
  }
  return lookup(name, options, callback);
};
