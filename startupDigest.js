const getGroupMembers = (groupName, groupsConfig = {}, users = new Map(), ownerName) => {
    const groupMembers = groupsConfig[groupName] || []
    const members = new Set()
    const ownerExplicitlyIncluded = ownerName && groupMembers.includes(ownerName)

    if (groupMembers.includes("$ALL")) {
        users.forEach((_, username) => {
            if (username === ownerName && !ownerExplicitlyIncluded) return
            members.add(username)
        })
    }

    groupMembers.forEach((member) => {
        if (member === "$ALL") return
        members.add(member)
    })

    if (!ownerExplicitlyIncluded && groupMembers.includes("$ALL")) {
        members.delete(ownerName)
    }

    return [...members]
}

const logGroupDigest = (config, users, logger, missingTokenWarnings = new Set()) => {
    const filters = config.filters || {}
    const groupsConfig = config.groups || {}

    Object.entries(filters).forEach(([libraryName, libraryGroups]) => {
        Object.keys(libraryGroups || {}).forEach((groupName) => {
            const members = getGroupMembers(groupName, groupsConfig, users, config.plex_owner_name)
            const resolvedMembers = members.filter((member) => users.has(member))
            const unresolvedMembers = members.filter((member) => !users.has(member))

            const memberList = resolvedMembers.length > 0 ? resolvedMembers.join(", ") : "none"
            const parts = [
                `Group digest (library='${libraryName}', group='${groupName}')`,
                `members=[${memberList}]`,
                `tokens resolved ${resolvedMembers.length}/${members.length}`,
            ]

            if (unresolvedMembers.length > 0) {
                parts.push(`missing tokens=[${unresolvedMembers.join(", ")}]`)
            }

            logger.info(parts.join("; "))

            unresolvedMembers.forEach((member) => {
                const key = `${groupName}|${member}`
                if (missingTokenWarnings.has(key)) return
                missingTokenWarnings.add(key)
                logger.warn(`no token for user '${member}' in group '${groupName}' -> will skip`)
            })
        })
    })
}

const logOwnerSafety = (config, logger) => {
    if (!config.plex_owner_name) return
    const owner = config.plex_owner_name
    const groupsContainingOwner = Object.entries(config.groups || {})
        .filter(([, members]) => members.includes(owner))
        .map(([name]) => name)

    if (groupsContainingOwner.length === 0) {
        logger.info(`Owner '${owner}' is not included in any group; no owner updates will be performed.`)
    } else {
        logger.warn(`Owner '${owner}' appears in groups: [${groupsContainingOwner.join(", ")}]. Proceeding per config.`)
    }
}

module.exports = { getGroupMembers, logGroupDigest, logOwnerSafety }
