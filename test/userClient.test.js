const test = require("node:test")
const assert = require("node:assert/strict")

const { buildUserHeaders, createUserClient } = require("../utils/userClient")

test("buildUserHeaders enforces per-user token isolation", () => {
    const headers = buildUserHeaders({
        token: "user-token-123456",
        username: "alice",
        clientIdentifier: "client-1",
        deviceNamePrefix: "Defaulter",
    })

    assert.equal(headers["X-Plex-Token"], "user-token-123456")
    assert.equal(headers["X-Plex-Client-Identifier"], "client-1")
    assert.equal(headers["X-Plex-Device-Name"], "Defaulter/alice")
    assert.equal(headers["X-Plex-Username"], "alice")
})

test("createUserClient passes headers to axios factory", () => {
    const captured = []
    const fakeAxios = {
        create: (options) => {
            captured.push(options)
            return { post: async () => ({ status: 200 }) }
        },
    }

    const client = createUserClient(
        {
            baseURL: "http://example.com",
            clientIdentifier: "client-abc",
            deviceNamePrefix: "Defaulter",
            axiosLib: fakeAxios,
        },
        { username: "bob", token: "bob-token-654321" }
    )

    assert.equal(typeof client.post, "function")
    assert.equal(captured.length, 1)
    const options = captured[0]
    assert.equal(options.baseURL, "http://example.com")
    assert.equal(options.headers["X-Plex-Token"], "bob-token-654321")
    assert.equal(options.headers["X-Plex-Client-Identifier"], "client-abc")
    assert.equal(options.headers["X-Plex-Device-Name"], "Defaulter/bob")
    assert.equal(options.headers["X-Plex-Username"], "bob")
})
