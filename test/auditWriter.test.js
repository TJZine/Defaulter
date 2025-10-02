const test = require("node:test")
const assert = require("node:assert/strict")
const fs = require("fs")
const path = require("path")
const os = require("os")

const AuditWriter = require("../auditWriter")

const readFirstFile = (dir, extension) => {
    const files = fs.readdirSync(dir).filter((file) => file.endsWith(extension))
    assert.ok(files.length > 0, `expected ${extension} file in ${dir}`)
    return path.join(dir, files[0])
}

test("audit writer produces JSON and CSV rows", async () => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "audit-writer-"))
    const writer = new AuditWriter(tempDir)

    writer.append({
        timestamp: "2024-01-01T00:00:00Z",
        libraryName: "Movies",
        ratingKey: "1",
        partId: "10",
        title: "Example",
        group: "groupA",
        user: "alice",
        actionType: "audio",
        fromStreamId: "100",
        fromLabel: "Old",
        toStreamId: "200",
        toLabel: "New",
        status: "success",
        reason: "",
        httpStatus: 200,
        durationMs: 123,
    })

    writer.append({
        timestamp: "2024-01-01T00:00:01Z",
        libraryName: "Movies",
        ratingKey: "2",
        partId: "20",
        title: "Second",
        group: "groupB",
        user: "bob",
        actionType: "subtitles",
        fromStreamId: "",
        fromLabel: "",
        toStreamId: "300",
        toLabel: "Enabled",
        status: "skipped",
        reason: "no_token",
        httpStatus: 403,
        durationMs: 456,
    })

    writer.close()

    await new Promise((resolve) => setTimeout(resolve, 20))

    const jsonPath = readFirstFile(tempDir, ".json")
    const jsonContent = fs.readFileSync(jsonPath, "utf-8")
    const data = JSON.parse(jsonContent)
    assert.equal(data.length, 2)
    assert.equal(data[0].user, "alice")
    assert.equal(data[1].status, "skipped")

    const csvPath = readFirstFile(tempDir, ".csv")
    const csvLines = fs.readFileSync(csvPath, "utf-8").trim().split(/\r?\n/)
    assert.equal(csvLines.length, 3)
    assert.equal(csvLines[0], "timestamp,libraryName,ratingKey,partId,title,group,user,actionType,fromStreamId,fromLabel,toStreamId,toLabel,status,reason,httpStatus,durationMs")
    assert.ok(csvLines[1].includes("alice"))
    assert.ok(csvLines[2].includes("no_token"))

    fs.rmSync(tempDir, { recursive: true, force: true })
})
