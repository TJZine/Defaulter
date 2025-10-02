const createUserSummaryEmitter = (config, logger) => {
    return (summary) => {
        if (!config.logUserSummary && !config.logJsonUserSummary) return

        if (config.logJsonUserSummary) {
            const jsonPayload = {
                event: "user_updates",
                library: summary.libraryName,
                group: summary.group,
                partId: summary.partId,
                ratingKey: summary.ratingKey,
                title: summary.title,
                users: summary.users.map((user) => {
                    const payload = { name: user.name, status: user.status }
                    if (user.audio) payload.audio = user.audio
                    if (user.subtitles) payload.subtitles = user.subtitles
                    if (user.reason) payload.reason = user.reason
                    if (user.httpStatus) payload.httpStatus = user.httpStatus
                    return payload
                }),
            }
            logger.info(JSON.stringify(jsonPayload))
        }

        if (config.logUserSummary) {
            const header = `User summary (library='${summary.libraryName}', group='${summary.group}', part=${summary.partId}, title='${summary.title}'):`
            const userSegments = summary.users.map((user) => {
                const detailSegments = []
                if (user.audio) detailSegments.push(`audio='${user.audio.label}' (id=${user.audio.id})`)
                if (user.subtitles) {
                    detailSegments.push(
                        user.subtitles.id === 0
                            ? "subtitles=disabled"
                            : `subtitles='${user.subtitles.label}' (id=${user.subtitles.id})`
                    )
                }

                const statusSuffix = user.reason && user.status !== "success"
                    ? `[${user.status}: ${user.reason}]`
                    : `[${user.status}]`

                const detailPrefix = detailSegments.length > 0 ? `${detailSegments.join(", ")} ` : ""
                return `${user.name}: ${detailPrefix}${statusSuffix}`.trim()
            })

            logger.info(`${header} ${userSegments.join(", ")}`)
        }
    }
}

module.exports = { createUserSummaryEmitter }
