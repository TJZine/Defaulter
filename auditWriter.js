const fs = require("fs")
const path = require("path")

const ensureDir = (dir) => {
    if (!dir) return
    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true })
    }
}

class AuditWriter {
    constructor(baseDir) {
        this.baseDir = baseDir
        this.jsonStream = null
        this.csvStream = null
        this.firstJsonEntry = true
        this.active = Boolean(baseDir)
        if (!this.active) return

        ensureDir(baseDir)
        const timestamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\..+/, "")
        const jsonPath = path.join(baseDir, `run-${timestamp}.json`)
        const csvPath = path.join(baseDir, `run-${timestamp}.csv`)

        this.jsonStream = fs.createWriteStream(jsonPath, { flags: "a" })
        this.jsonStream.write("[\n")

        this.csvStream = fs.createWriteStream(csvPath, { flags: "a" })
        this.csvStream.write(
            "timestamp,libraryName,ratingKey,partId,title,group,user,actionType,fromStreamId,fromLabel,toStreamId,toLabel,status,reason,httpStatus,durationMs\n"
        )
    }

    append(entry) {
        if (!this.active) return
        const serialized = JSON.stringify(entry)
        if (!this.firstJsonEntry) {
            this.jsonStream.write(",\n")
        }
        this.jsonStream.write(serialized)
        this.firstJsonEntry = false

        const escapeCsv = (value) => {
            if (value === undefined || value === null) return ""
            const stringValue = String(value)
            if (/[",\n]/.test(stringValue)) {
                return `"${stringValue.replace(/"/g, '""')}"`
            }
            return stringValue
        }

        const csvValues = [
            entry.timestamp,
            entry.libraryName,
            entry.ratingKey,
            entry.partId,
            entry.title,
            entry.group,
            entry.user,
            entry.actionType,
            entry.fromStreamId,
            entry.fromLabel,
            entry.toStreamId,
            entry.toLabel,
            entry.status,
            entry.reason,
            entry.httpStatus,
            entry.durationMs,
        ]
            .map((value) => escapeCsv(value))
            .join(",")

        this.csvStream.write(`${csvValues}\n`)
    }

    close() {
        if (!this.active) return
        if (this.jsonStream) {
            this.jsonStream.write("\n]\n")
            this.jsonStream.end()
        }
        if (this.csvStream) {
            this.csvStream.end()
        }
    }
}

module.exports = AuditWriter
