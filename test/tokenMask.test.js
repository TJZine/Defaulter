const test = require("node:test")
const assert = require("node:assert")

const { maskToken } = require("../utils/token")

test("maskToken masks everything but last six characters", () => {
    assert.equal(maskToken("1234567890abcdef"), "***…abcdef")
})

test("maskToken handles short tokens", () => {
    assert.equal(maskToken("123"), "***")
})

test("maskToken handles empty values", () => {
    assert.equal(maskToken(""), "(none)")
    assert.equal(maskToken(null), "(none)")
})
