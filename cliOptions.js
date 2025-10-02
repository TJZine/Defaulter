const toCamelCase = (input) =>
    input
        .replace(/^no-/, "")
        .split(/[-_]/)
        .map((part, index) => (index === 0 ? part : part.charAt(0).toUpperCase() + part.slice(1)))
        .join("")

const parseValue = (raw) => {
    if (raw === undefined) return true
    const normalized = raw.trim().toLowerCase()
    if (["true", "1", "yes", "on"].includes(normalized)) return true
    if (["false", "0", "no", "off"].includes(normalized)) return false
    return raw
}

const args = process.argv.slice(2)
const positional = []
const flags = {}

for (let index = 0; index < args.length; index++) {
    const arg = args[index]
    if (!arg.startsWith("--")) {
        positional.push(arg)
        continue
    }

    const trimmed = arg.slice(2)
    if (trimmed.length === 0) continue

    let key
    let value

    if (trimmed.includes("=")) {
        const [rawKey, rawValue] = trimmed.split(/=(.*)/)
        key = rawKey
        value = parseValue(rawValue)
    } else {
        key = trimmed
        const next = args[index + 1]
        if (next && !next.startsWith("--")) {
            value = parseValue(next)
            index++
        } else if (trimmed.startsWith("no-")) {
            value = false
        } else {
            value = true
        }
    }

    const camelKey = toCamelCase(key)
    flags[camelKey] = value
}

const getFlag = (name) => {
    if (Object.prototype.hasOwnProperty.call(flags, name)) return flags[name]
    return undefined
}

module.exports = {
    positional,
    flags,
    getFlag,
}
