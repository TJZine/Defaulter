const { createUserClient } = require("./utils/userClient")

const describeActionType = (stream) => {
    const audio = Boolean(stream.audioStreamId)
    const subtitles = stream.subtitleStreamId !== undefined && stream.subtitleStreamId >= 0
    if (audio && subtitles) return "audio+subtitles"
    if (audio) return "audio"
    if (subtitles) return "subtitles"
    return "none"
}

const buildStreamTransition = (stream) => {
    const fromIds = []
    const fromLabels = []
    const toIds = []
    const toLabels = []

    if (stream.audioStreamId) {
        fromIds.push(stream.fromAudioStreamId ?? "")
        fromLabels.push(stream.fromAudioStreamLabel || "")
        toIds.push(stream.audioStreamId)
        toLabels.push(stream.audioStreamLabel || "")
    }

    if (stream.subtitleStreamId !== undefined && stream.subtitleStreamId >= 0) {
        fromIds.push(stream.fromSubtitleStreamId ?? "")
        fromLabels.push(stream.fromSubtitleStreamLabel || "")
        toIds.push(stream.subtitleStreamId)
        toLabels.push(stream.subtitleStreamLabel || "")
    }

    return {
        fromStreamId: fromIds.join("|") || null,
        fromLabel: fromLabels.join(" | ") || null,
        toStreamId: toIds.join("|") || null,
        toLabel: toLabels.join(" | ") || null,
    }
}

const createStreamUpdater = ({
    config,
    logger,
    userTokens,
    auditWriter,
    emitUserSummary,
    delay,
    maskToken,
    recordSkipForUser,
    runStats,
    onFatalError,
    clientFactory,
}) => {
    const buildAuditEntry = ({
        timestamp,
        libraryName,
        stream,
        group,
        username,
        status,
        reason,
        httpStatus,
        durationMs,
    }) => {
        const transition = buildStreamTransition(stream)
        auditWriter.append({
            timestamp,
            libraryName,
            ratingKey: stream.ratingKey,
            partId: stream.partId,
            title: stream.title,
            group,
            user: username,
            actionType: describeActionType(stream),
            ...transition,
            status,
            reason: reason ?? null,
            httpStatus: httpStatus ?? null,
            durationMs: typeof durationMs === "number" ? durationMs : 0,
        })
    }

    const getClient = (username, token) => {
        if (clientFactory) return clientFactory(username, token)
        return createUserClient(
            {
                baseURL: config.plex_server_url,
                clientIdentifier: config.plex_client_identifier,
            },
            { username, token }
        )
    }

    const updateDefaultStreamsPerItem = async (libraryName, streamsToUpdate, usersWithAccess, options = {}) => {
        const { dryRun = false, maxAttempts = 10 } = options

        for (const group of Object.keys(streamsToUpdate)) {
            for (const stream of streamsToUpdate[group]) {
                const usernames = usersWithAccess.get(group)
                if (!usernames || usernames.length === 0) {
                    logger.warn(`No users found in group '${group}'. Skipping update.`)
                    continue
                }

                const summary = {
                    libraryName,
                    group,
                    partId: stream.partId,
                    ratingKey: stream.ratingKey,
                    title: stream.title,
                    users: [],
                }

                const queryParams = new URLSearchParams()
                if (stream.audioStreamId) queryParams.append("audioStreamID", stream.audioStreamId)
                if (stream.subtitleStreamId >= 0) queryParams.append("subtitleStreamID", stream.subtitleStreamId)

                for (const username of usernames) {
                    const token = userTokens.get(username)
                    const userSummary = { name: username }
                    if (stream.audioStreamId) {
                        userSummary.audio = { id: stream.audioStreamId, label: stream.audioStreamLabel || "" }
                    }
                    if (stream.subtitleStreamId >= 0) {
                        userSummary.subtitles = {
                            id: stream.subtitleStreamId,
                            label: stream.subtitleStreamLabel || "",
                        }
                    }

                    if (!token) {
                        logger.warn(`No access token found for user ${username}. Skipping update.`)
                        userSummary.status = "skipped"
                        userSummary.reason = "no_token"
                        summary.users.push(userSummary)
                        buildAuditEntry({
                            timestamp: new Date().toISOString(),
                            libraryName,
                            stream,
                            group,
                            username,
                            status: "skipped",
                            reason: "no_token",
                            httpStatus: null,
                            durationMs: 0,
                        })
                        continue
                    }

                    if (dryRun) {
                        userSummary.status = "dry_run"
                        summary.users.push(userSummary)
                        buildAuditEntry({
                            timestamp: new Date().toISOString(),
                            libraryName,
                            stream,
                            group,
                            username,
                            status: "dry_run",
                            reason: null,
                            httpStatus: null,
                            durationMs: 0,
                        })
                        continue
                    }

                    const userClient = getClient(username, token)

                    runStats.processed++
                    const startTime = Date.now()
                    let skipUpdate = false
                    let httpStatus = null
                    let reason = null

                    let response
                    let attempt = 0
                    let fatalError = false
                    while (attempt < maxAttempts && !response && !skipUpdate) {
                        try {
                            response = await userClient.post(`/library/parts/${stream.partId}?${queryParams.toString()}`)
                            httpStatus = response.status
                            if (response.status === 200) {
                                runStats.succeeded++
                                userSummary.status = "success"
                                userSummary.httpStatus = response.status
                            } else {
                                runStats.failed++
                                userSummary.status = "error"
                                reason = `HTTP ${response.status}`
                                userSummary.reason = reason
                                userSummary.httpStatus = response.status
                            }
                        } catch (error) {
                            httpStatus = error.response?.status ?? null
                            if (httpStatus === 403 && config.skipInaccessibleItems) {
                                skipUpdate = true
                                recordSkipForUser(username)
                                reason = "HTTP 403"
                                userSummary.status = "skipped"
                                userSummary.reason = "HTTP 403"
                                userSummary.httpStatus = httpStatus
                                const itemLabel = stream.title
                                    ? `'${stream.title}' (Part ID ${stream.partId})`
                                    : `Part ID ${stream.partId}`
                                logger.warn(
                                    `Skipping item ${itemLabel} for user ${username} in group ${group}: inaccessible (HTTP 403, token ${maskToken(
                                        token
                                    )}).`
                                )
                            } else {
                                reason = error.message || error.response?.statusText || "Unknown error"
                                userSummary.status = "error"
                                userSummary.reason = reason
                                userSummary.httpStatus = httpStatus
                                const messageSuffix =
                                    httpStatus === 403
                                        ? ". This could be because of age ratings, ensure they can access ALL items in the library"
                                        : ""
                                if (attempt < maxAttempts - 1) {
                                    logger.error(
                                        `Attempt ${attempt + 1}/${maxAttempts} failed for user ${username} in group ${group}${messageSuffix}: ${reason}. Retrying in 30 sec...`
                                    )
                                    attempt++
                                    await delay(30000)
                                } else {
                                    fatalError = true
                                    runStats.failed++
                                    logger.error(
                                        `All attempts failed for user ${username} in group ${group}. Reason: ${reason}. Exiting application.`
                                    )
                                    break
                                }
                            }
                        }
                    }

                    const duration = Date.now() - startTime
                    buildAuditEntry({
                        timestamp: new Date(startTime).toISOString(),
                        libraryName,
                        stream,
                        group,
                        username,
                        status: skipUpdate ? "skipped" : userSummary.status || "error",
                        reason,
                        httpStatus,
                        durationMs: duration,
                    })

                    if (!skipUpdate && !userSummary.status) {
                        userSummary.status = httpStatus === 200 ? "success" : "error"
                        if (httpStatus !== 200) userSummary.reason = reason
                    }

                    if (skipUpdate) {
                        await delay(100)
                        summary.users.push(userSummary)
                        continue
                    }

                    const statusForLog = httpStatus
                    const audioMessage = stream.audioStreamId ? `Audio ID ${stream.audioStreamId}` : ""
                    const subtitleMessage = stream.subtitleStreamId >= 0 ? `Subtitle ID ${stream.subtitleStreamId}` : ""
                    const updateMessage = [audioMessage, subtitleMessage].filter(Boolean).join(" and ")
                    logger.debug(
                        `Update ${updateMessage} for user ${username} in group ${group}: ${
                            statusForLog === 200 ? "SUCCESS" : "FAIL"
                        }`
                    )

                    summary.users.push(userSummary)
                    if (fatalError) {
                        emitUserSummary(summary)
                        if (onFatalError) onFatalError()
                        return false
                    }
                    await delay(100)
                }

                logger.info(`Part ID ${stream.partId}: update complete for group ${group}`)
                emitUserSummary(summary)
            }
        }
        return true
    }

    return {
        updateDefaultStreamsPerItem,
        describeActionType,
        buildStreamTransition,
    }
}

module.exports = { createStreamUpdater, describeActionType, buildStreamTransition }
