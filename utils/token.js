const maskToken = (token) => {
    if (!token) return "(none)"

    const normalized = token.toString().trim()
    if (normalized.length === 0) return "(none)"
    if (normalized.length <= 6) return "*".repeat(normalized.length)
    return `***…${normalized.slice(-6)}`
}

module.exports = { maskToken }
