const test = require("node:test")
const assert = require("node:assert/strict")

const loadCliOptions = () => {
    delete require.cache[require.resolve("../cliOptions")]
    return require("../cliOptions")
}

test("cliOptions parses booleans, strings, and positional arguments", () => {
    const originalArgv = process.argv
    process.argv = [
        originalArgv[0],
        originalArgv[1],
        "--log-user-summary",
        "--audit-dir",
        "/tmp/audit",
        "./logs",
        "./config.yaml",
        "timestamps.json",
    ]

    const options = loadCliOptions()

    assert.equal(options.getFlag("logUserSummary"), true)
    assert.equal(options.getFlag("auditDir"), "/tmp/audit")
    assert.deepEqual(options.positional, ["./logs", "./config.yaml", "timestamps.json"])

    process.argv = originalArgv
    loadCliOptions()
})

test("cliOptions parses negated booleans", () => {
    const originalArgv = process.argv
    process.argv = [originalArgv[0], originalArgv[1], "--no-log-user-summary", "--log-json-user-summary=false"]

    const options = loadCliOptions()

    assert.equal(options.getFlag("logUserSummary"), false)
    assert.equal(options.getFlag("logJsonUserSummary"), false)

    process.argv = originalArgv
    loadCliOptions()
})
