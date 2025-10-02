const test = require("node:test")
const assert = require("node:assert")

const { logGroupDigest, getGroupMembers, logOwnerSafety } = require("../startupDigest")

const createFakeLogger = () => {
    const info = []
    const warn = []
    return {
        info: (message) => info.push(message),
        warn: (message) => warn.push(message),
        infoMessages: info,
        warnMessages: warn,
    }
}

test("logGroupDigest reports members and resolved tokens", () => {
    const config = {
        plex_owner_name: "owner",
        groups: { crew: ["alice", "bob"] },
        filters: { Movies: { crew: {} } },
    }
    const users = new Map([
        ["alice", "token-a"],
        ["bob", "token-b"],
    ])
    const logger = createFakeLogger()

    logGroupDigest(config, users, logger)

    assert.equal(logger.infoMessages.length, 1)
    assert.equal(
        logger.infoMessages[0],
        "Group digest (library='Movies', group='crew'); members=[alice, bob]; tokens resolved 2/2"
    )
    assert.equal(logger.warnMessages.length, 0)
})

test("logGroupDigest warns when tokens are missing", () => {
    const config = {
        plex_owner_name: "owner",
        groups: { crew: ["alice", "bob"] },
        filters: { Movies: { crew: {} } },
    }
    const users = new Map([["alice", "token-a"]])
    const logger = createFakeLogger()

    logGroupDigest(config, users, logger)

    assert.equal(logger.infoMessages.length, 1)
    assert.equal(
        logger.infoMessages[0],
        "Group digest (library='Movies', group='crew'); members=[alice]; tokens resolved 1/2; missing tokens=[bob]"
    )
    assert.deepEqual(logger.warnMessages, ["no token for user 'bob' in group 'crew' -> will skip"])
})

test("getGroupMembers excludes owner unless explicitly included", () => {
    const groupsConfig = { everyone: ["$ALL"] }
    const users = new Map([
        ["owner", "token-owner"],
        ["alice", "token-a"],
    ])

    const members = getGroupMembers("everyone", groupsConfig, users, "owner")

    assert.deepEqual(members.sort(), ["alice"])
})

test("logOwnerSafety warns when owner is configured", () => {
    const config = { plex_owner_name: "owner", groups: { crew: ["owner"] } }
    const logger = createFakeLogger()

    logOwnerSafety(config, logger)

    assert.deepEqual(logger.warnMessages, ["Owner 'owner' appears in groups: [crew]. Proceeding per config."])
})

test("logOwnerSafety informs when owner absent", () => {
    const config = { plex_owner_name: "owner", groups: { crew: ["alice"] } }
    const logger = createFakeLogger()

    logOwnerSafety(config, logger)

    assert.deepEqual(logger.infoMessages, ["Owner 'owner' is not included in any group; no owner updates will be performed."])
})
