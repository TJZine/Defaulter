const test = require("node:test")
const assert = require("node:assert")

const { createUserSummaryEmitter } = require("../summaryEmitter")

test("JSON summaries emit valid JSON with expected shape", () => {
    const infoMessages = []
    const emitter = createUserSummaryEmitter(
        { logJsonUserSummary: true, logUserSummary: false },
        { info: (message) => infoMessages.push(message) }
    )

    emitter({
        libraryName: "Movies",
        group: "crew",
        partId: 123,
        ratingKey: 456,
        title: "Example",
        users: [
            { name: "alice", status: "success", audio: { id: 10, label: "AC3" } },
            { name: "bob", status: "skipped", reason: "HTTP 403" },
        ],
    })

    assert.equal(infoMessages.length, 1)
    const parsed = JSON.parse(infoMessages[0])
    assert.equal(parsed.event, "user_updates")
    assert.equal(parsed.library, "Movies")
    assert.equal(parsed.group, "crew")
    assert.equal(parsed.partId, 123)
    assert.equal(parsed.ratingKey, 456)
    assert.equal(parsed.title, "Example")
    assert.deepEqual(parsed.users, [
        { name: "alice", status: "success", audio: { id: 10, label: "AC3" } },
        { name: "bob", status: "skipped", reason: "HTTP 403" },
    ])
})

test("Human summaries emit single line with statuses", () => {
    const infoMessages = []
    const emitter = createUserSummaryEmitter(
        { logJsonUserSummary: false, logUserSummary: true },
        { info: (message) => infoMessages.push(message) }
    )

    emitter({
        libraryName: "Movies",
        group: "crew",
        partId: 123,
        ratingKey: 456,
        title: "Example",
        users: [
            { name: "alice", status: "success" },
            { name: "bob", status: "skipped", reason: "HTTP 403" },
            { name: "carol", status: "error", reason: "timeout" },
        ],
    })

    assert.equal(infoMessages.length, 1)
    assert.equal(
        infoMessages[0],
        "User summary (library='Movies', group='crew', part=123, title='Example'): alice: [success], bob: [skipped: HTTP 403], carol: [error: timeout]"
    )
})

test("Human summaries include stream details when available", () => {
    const infoMessages = []
    const emitter = createUserSummaryEmitter(
        { logJsonUserSummary: false, logUserSummary: true },
        { info: (message) => infoMessages.push(message) }
    )

    emitter({
        libraryName: "Movies",
        group: "crew",
        partId: 42,
        ratingKey: 99,
        title: "Detailed",
        users: [
            { name: "alice", status: "success", audio: { id: 10, label: "AC3" } },
            { name: "bob", status: "success", subtitles: { id: 22, label: "English CC" } },
            { name: "carol", status: "success", subtitles: { id: 0, label: "" } },
        ],
    })

    assert.equal(infoMessages.length, 1)
    assert.equal(
        infoMessages[0],
        "User summary (library='Movies', group='crew', part=42, title='Detailed'): alice: audio='AC3' (id=10) [success], bob: subtitles='English CC' (id=22) [success], carol: subtitles=disabled [success]"
    )
})
