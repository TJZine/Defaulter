const test = require("node:test")
const assert = require("node:assert/strict")

const { createStreamUpdater } = require("../streamUpdater")

const createLogger = () => ({
    info: () => {},
    warn: () => {},
    error: () => {},
    debug: () => {},
})

const createCapturingLogger = () => {
    const info = []
    const warn = []
    const error = []
    const debug = []
    return {
        info: (message) => info.push(message),
        warn: (message) => warn.push(message),
        error: (message) => error.push(message),
        debug: (message) => debug.push(message),
        infoMessages: info,
        warnMessages: warn,
        errorMessages: error,
        debugMessages: debug,
    }
}

const createRunStats = () => ({ processed: 0, succeeded: 0, failed: 0, skipped: 0, skippedByUser: {} })

const createRecordSkip = (runStats) => (username) => {
    runStats.skipped++
    runStats.skippedByUser[username] = (runStats.skippedByUser[username] || 0) + 1
}

test("success path records audit entry and uses per-user token", async () => {
    const auditEntries = []
    const summaries = []
    const tokensUsed = []
    const config = { plex_server_url: "http://plex", plex_client_identifier: "client", skipInaccessibleItems: false }
    const runStats = createRunStats()

    const { updateDefaultStreamsPerItem } = createStreamUpdater({
        config,
        logger: createLogger(),
        userTokens: new Map([["alice", "alice-token"], ["owner", "owner-token"]]),
        auditWriter: { append: (entry) => auditEntries.push(entry) },
        emitUserSummary: (summary) => summaries.push(summary),
        delay: async () => {},
        maskToken: (token) => token,
        recordSkipForUser: createRecordSkip(runStats),
        runStats,
        onFatalError: () => {
            throw new Error("fatal should not be called")
        },
        clientFactory: (username, token) => {
            tokensUsed.push(token)
            return { post: async () => ({ status: 200 }) }
        },
    })

    const usersWithAccess = new Map([["groupA", ["alice"]]])
    const stream = {
        partId: 10,
        ratingKey: 100,
        title: "Example",
        audioStreamId: 200,
        audioStreamLabel: "Stereo",
    }

    await updateDefaultStreamsPerItem("Movies", { groupA: [stream] }, usersWithAccess)

    assert.deepEqual(tokensUsed, ["alice-token"])
    assert.equal(runStats.processed, 1)
    assert.equal(runStats.succeeded, 1)
    assert.equal(auditEntries.length, 1)
    assert.equal(auditEntries[0].status, "success")
    assert.equal(auditEntries[0].httpStatus, 200)
    assert.equal(auditEntries[0].reason, null)
    assert.ok(auditEntries[0].durationMs >= 0)
    assert.equal(summaries.length, 1)
    assert.equal(summaries[0].users[0].status, "success")
})

test("403 inaccessible is skipped with audit row and human summary", async () => {
    const auditEntries = []
    const summaries = []
    const runStats = createRunStats()
    const config = { plex_server_url: "http://plex", plex_client_identifier: "client", skipInaccessibleItems: true }

    const { updateDefaultStreamsPerItem } = createStreamUpdater({
        config,
        logger: createLogger(),
        userTokens: new Map([["bob", "bob-token"]]),
        auditWriter: { append: (entry) => auditEntries.push(entry) },
        emitUserSummary: (summary) => summaries.push(summary),
        delay: async () => {},
        maskToken: (token) => `masked-${token}`,
        recordSkipForUser: createRecordSkip(runStats),
        runStats,
        onFatalError: () => {
            throw new Error("fatal should not be called")
        },
        clientFactory: () => ({
            post: async () => {
                const error = new Error("Forbidden")
                error.response = { status: 403 }
                throw error
            },
        }),
    })

    const usersWithAccess = new Map([["groupA", ["bob"]]])
    const stream = { partId: 11, ratingKey: 101, title: "Item", audioStreamId: 1 }

    await updateDefaultStreamsPerItem("Movies", { groupA: [stream] }, usersWithAccess, { maxAttempts: 1 })

    assert.equal(runStats.skipped, 1)
    assert.equal(auditEntries.length, 1)
    assert.equal(auditEntries[0].status, "skipped")
    assert.equal(auditEntries[0].reason, "HTTP 403")
    assert.equal(auditEntries[0].httpStatus, 403)
    assert.equal(summaries.length, 1)
    assert.equal(summaries[0].users.length, 1)
    const userSummary = summaries[0].users[0]
    assert.equal(userSummary.name, "bob")
    assert.equal(userSummary.status, "skipped")
    assert.equal(userSummary.reason, "HTTP 403")
    assert.equal(userSummary.httpStatus, 403)
})

test("500 error writes audit row and triggers fatal handler", async () => {
    const auditEntries = []
    const runStats = createRunStats()
    let fatalCalled = false
    const config = { plex_server_url: "http://plex", plex_client_identifier: "client", skipInaccessibleItems: false }

    const { updateDefaultStreamsPerItem } = createStreamUpdater({
        config,
        logger: createLogger(),
        userTokens: new Map([["carol", "carol-token"]]),
        auditWriter: { append: (entry) => auditEntries.push(entry) },
        emitUserSummary: () => {},
        delay: async () => {},
        maskToken: (token) => token,
        recordSkipForUser: createRecordSkip(runStats),
        runStats,
        onFatalError: () => {
            fatalCalled = true
        },
        clientFactory: () => ({
            post: async () => {
                const error = new Error("Server error")
                error.response = { status: 500 }
                throw error
            },
        }),
    })

    const usersWithAccess = new Map([["groupA", ["carol"]]])
    const stream = { partId: 12, ratingKey: 102, title: "Item" }

    await updateDefaultStreamsPerItem("Movies", { groupA: [stream] }, usersWithAccess, { maxAttempts: 1 })

    assert.equal(fatalCalled, true)
    assert.equal(auditEntries.length, 1)
    assert.equal(auditEntries[0].status, "error")
    assert.equal(auditEntries[0].httpStatus, 500)
    assert.equal(auditEntries[0].reason, "Server error")
})

test("dry run records audit entries without invoking client", async () => {
    const auditEntries = []
    const summaries = []
    const tokensUsed = []

    const { updateDefaultStreamsPerItem } = createStreamUpdater({
        config: { plex_server_url: "http://plex", plex_client_identifier: "client", skipInaccessibleItems: false },
        logger: createLogger(),
        userTokens: new Map([["dave", "dave-token"]]),
        auditWriter: { append: (entry) => auditEntries.push(entry) },
        emitUserSummary: (summary) => summaries.push(summary),
        delay: async () => {},
        maskToken: (token) => token,
        recordSkipForUser: () => {},
        runStats: createRunStats(),
        onFatalError: () => {
            throw new Error("fatal should not be called")
        },
        clientFactory: (username, token) => {
            tokensUsed.push(token)
            return { post: async () => ({ status: 200 }) }
        },
    })

    const usersWithAccess = new Map([["groupA", ["dave"]]])
    const stream = { partId: 13, ratingKey: 103, title: "Dry", audioStreamId: 1 }

    await updateDefaultStreamsPerItem("Movies", { groupA: [stream] }, usersWithAccess, { dryRun: true })

    assert.equal(tokensUsed.length, 0)
    assert.equal(auditEntries.length, 1)
    assert.equal(auditEntries[0].status, "dry_run")
    assert.equal(auditEntries[0].httpStatus, null)
    assert.equal(auditEntries[0].durationMs, 0)
    assert.equal(summaries.length, 1)
    assert.equal(summaries[0].users[0].status, "dry_run")
})

test("missing token is recorded as skipped", async () => {
    const auditEntries = []
    const summaries = []

    const { updateDefaultStreamsPerItem } = createStreamUpdater({
        config: { plex_server_url: "http://plex", plex_client_identifier: "client", skipInaccessibleItems: false },
        logger: createLogger(),
        userTokens: new Map(),
        auditWriter: { append: (entry) => auditEntries.push(entry) },
        emitUserSummary: (summary) => summaries.push(summary),
        delay: async () => {},
        maskToken: (token) => token,
        recordSkipForUser: () => {},
        runStats: createRunStats(),
        onFatalError: () => {
            throw new Error("fatal should not be called")
        },
    })

    const usersWithAccess = new Map([["groupA", ["erin"]]])
    const stream = { partId: 14, ratingKey: 104, title: "Skip", audioStreamId: 1 }

    await updateDefaultStreamsPerItem("Movies", { groupA: [stream] }, usersWithAccess)

    assert.equal(auditEntries.length, 1)
    assert.equal(auditEntries[0].status, "skipped")
    assert.equal(auditEntries[0].reason, "no_token")
    assert.equal(auditEntries[0].httpStatus, null)
    assert.equal(summaries.length, 1)
    const summary = summaries[0].users[0]
    assert.equal(summary.name, "erin")
    assert.equal(summary.status, "skipped")
    assert.equal(summary.reason, "no_token")
})

test("owner token is ignored when owner not in group", async () => {
    const auditEntries = []
    const tokensUsed = []

    const { updateDefaultStreamsPerItem } = createStreamUpdater({
        config: { plex_server_url: "http://plex", plex_client_identifier: "client", skipInaccessibleItems: false },
        logger: createLogger(),
        userTokens: new Map([
            ["alice", "alice-token"],
            ["owner", "owner-token"],
        ]),
        auditWriter: { append: (entry) => auditEntries.push(entry) },
        emitUserSummary: () => {},
        delay: async () => {},
        maskToken: (token) => token,
        recordSkipForUser: () => {},
        runStats: createRunStats(),
        onFatalError: () => {
            throw new Error("fatal should not be called")
        },
        clientFactory: (username, token) => {
            tokensUsed.push({ username, token })
            return { post: async () => ({ status: 200 }) }
        },
    })

    const usersWithAccess = new Map([["groupA", ["alice"]]])
    const stream = { partId: 15, ratingKey: 105, title: "Ownerless" }

    await updateDefaultStreamsPerItem("Movies", { groupA: [stream] }, usersWithAccess)

    assert.deepEqual(tokensUsed, [{ username: "alice", token: "alice-token" }])
    assert.equal(auditEntries.length, 1)
    assert.equal(auditEntries[0].user, "alice")
})

test("403 warnings mask user tokens", async () => {
    const auditEntries = []
    const logger = createCapturingLogger()

    const { updateDefaultStreamsPerItem } = createStreamUpdater({
        config: { plex_server_url: "http://plex", plex_client_identifier: "client", skipInaccessibleItems: true },
        logger,
        userTokens: new Map([["bob", "bob-secret-token"]]),
        auditWriter: { append: (entry) => auditEntries.push(entry) },
        emitUserSummary: () => {},
        delay: async () => {},
        maskToken: () => "MASKED",
        recordSkipForUser: () => {},
        runStats: createRunStats(),
        onFatalError: () => {},
        clientFactory: () => ({
            post: async () => {
                const error = new Error("Forbidden")
                error.response = { status: 403 }
                throw error
            },
        }),
    })

    const usersWithAccess = new Map([["groupA", ["bob"]]])
    const stream = { partId: 16, ratingKey: 106, title: "Mask" }

    await updateDefaultStreamsPerItem("Movies", { groupA: [stream] }, usersWithAccess, { maxAttempts: 1 })

    assert.ok(logger.warnMessages.some((message) => message.includes("MASKED")))
    assert.ok(!logger.warnMessages.some((message) => message.includes("bob-secret-token")))
})

test("each user update uses the correct token", async () => {
    const auditEntries = []
    const tokensUsed = []
    const runStats = createRunStats()

    const { updateDefaultStreamsPerItem } = createStreamUpdater({
        config: { plex_server_url: "http://plex", plex_client_identifier: "client", skipInaccessibleItems: false },
        logger: createLogger(),
        userTokens: new Map([
            ["alice", "alice-token"],
            ["bob", "bob-token"],
        ]),
        auditWriter: { append: (entry) => auditEntries.push(entry) },
        emitUserSummary: () => {},
        delay: async () => {},
        maskToken: (token) => token,
        recordSkipForUser: () => {},
        runStats,
        onFatalError: () => {},
        clientFactory: (username, token) => {
            tokensUsed.push({ username, token })
            return { post: async () => ({ status: 200 }) }
        },
    })

    const usersWithAccess = new Map([["groupA", ["alice", "bob"]]])
    const stream = { partId: 17, ratingKey: 107, title: "Multi" }

    await updateDefaultStreamsPerItem("Movies", { groupA: [stream] }, usersWithAccess)

    assert.deepEqual(tokensUsed, [
        { username: "alice", token: "alice-token" },
        { username: "bob", token: "bob-token" },
    ])
    assert.equal(runStats.processed, 2)
    assert.equal(runStats.succeeded, 2)
    assert.equal(auditEntries.length, 2)
    assert.deepEqual(
        auditEntries.map((entry) => entry.user).sort(),
        ["alice", "bob"]
    )
})
