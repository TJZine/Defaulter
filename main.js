const express = require("express")
const fs = require("fs")
const axios = require("axios")
const logger = require("./logger")
const cron = require("node-cron")
const cronValidator = require("cron-validator")
const xml2js = require("xml2js")
const cliOptions = require("./cliOptions")
const AuditWriter = require("./auditWriter")
const loadAndValidateYAML = require("./configBuilder")

const parseBoolean = (value) => {
    if (typeof value === "boolean") return value
    if (typeof value === "string") {
        const normalized = value.trim().toLowerCase()
        if (["1", "true", "yes", "on"].includes(normalized)) return true
        if (["0", "false", "no", "off"].includes(normalized)) return false
    }
    return undefined
}

const config = loadAndValidateYAML()

const getBooleanOption = (flagName, envName, configKey, defaultValue = false) => {
    const cliValue = cliOptions.getFlag(flagName)
    if (typeof cliValue === "boolean") return cliValue
    const envValue = parseBoolean(process.env[envName])
    if (typeof envValue === "boolean") return envValue
    if (typeof config[configKey] === "boolean") return config[configKey]
    return defaultValue
}

const getStringOption = (flagName, envName, configKey) => {
    const cliValue = cliOptions.getFlag(flagName)
    if (typeof cliValue === "string" && cliValue.length > 0) return cliValue
    const envValue = process.env[envName]
    if (envValue && envValue.length > 0) return envValue
    if (typeof config[configKey] === "string" && config[configKey].length > 0) return config[configKey]
    return undefined
}

config.skipInaccessibleItems = getBooleanOption("skipInaccessibleItems", "SKIP_INACCESSIBLE_ITEMS", "skipInaccessibleItems", false)
config.dry_run = getBooleanOption("dryRun", "DRY_RUN", "dry_run", config.dry_run)
config.logUserSummary = getBooleanOption("logUserSummary", "LOG_USER_SUMMARY", "logUserSummary", false)
config.logJsonUserSummary = getBooleanOption("logJsonUserSummary", "LOG_JSON_USER_SUMMARY", "logJsonUserSummary", false)
config.auditDir = getStringOption("auditDir", "AUDIT_DIR", "auditDir")

const timestampsFile =
    getStringOption("timestamps", "TIMESTAMPS_FILE", undefined) || cliOptions.positional[2] || "./last_run_timestamps.json"
const app = express()
app.use(express.json())

const STREAM_TYPES = { video: 1, audio: 2, subtitles: 3 }
const LIBRARIES = new Map()
const USERS = new Map()
const missingTokenWarnings = new Set()
const runStats = {
    processed: 0,
    succeeded: 0,
    failed: 0,
    skipped: 0,
    skippedByUser: {},
}

const resetRunStats = () => {
    runStats.processed = 0
    runStats.succeeded = 0
    runStats.failed = 0
    runStats.skipped = 0
    runStats.skippedByUser = {}
}

const recordSkipForUser = (username) => {
    runStats.skipped++
    runStats.skippedByUser[username] = (runStats.skippedByUser[username] || 0) + 1
}

const logRunSummary = () => {
    logger.info(
        `Run summary: processed=${runStats.processed}, succeeded=${runStats.succeeded}, failed=${runStats.failed}, skipped=${runStats.skipped}`
    )
    if (Object.keys(runStats.skippedByUser).length === 0) {
        logger.info("skippedInaccessibleItemsByUser: none")
        return
    }
    Object.entries(runStats.skippedByUser).forEach(([user, count]) => {
        logger.info(`skippedInaccessibleItemsByUser[${user}] = ${count}`)
    })
}

const getGroupMembers = (groupName) => {
    const groupMembers = config.groups?.[groupName] || []
    if (groupMembers.includes("$ALL")) {
        return [...USERS.keys()]
    }
    return [...new Set(groupMembers)]
}

const logGroupDigest = () => {
    Object.entries(config.filters || {}).forEach(([libraryName, libraryGroups]) => {
        logger.info(`Group digest (library='${libraryName}'):`)
        Object.keys(libraryGroups || {}).forEach((groupName) => {
            const members = getGroupMembers(groupName)
            const resolvedMembers = members.filter((member) => USERS.has(member))
            const missingMembers = members.filter((member) => !USERS.has(member))
            const memberList = members.length > 0 ? members.join(",") : "none"

            logger.info(`  - ${groupName}: members=${memberList} (${members.length})`)
            logger.info(
                `    resolvedTokens: ${resolvedMembers.length}/${members.length || 0} ${
                    missingMembers.length === 0 ? "ok" : "missing"
                }; restrictedAccess: ${missingMembers.length} (updates skipped for missing tokens)`
            )

            missingMembers.forEach((member) => {
                const key = `${groupName}|${member}`
                if (missingTokenWarnings.has(key)) return
                missingTokenWarnings.add(key)
                logger.warn(`no token for user '${member}' in group '${groupName}' -> will skip updates for this user`)
            })
        })
    })
}

const logOwnerSafety = () => {
    if (!config.plex_owner_name) return
    const owner = config.plex_owner_name
    const groupsContainingOwner = Object.entries(config.groups || {})
        .filter(([, members]) => members.includes(owner) || members.includes("$ALL"))
        .map(([name]) => name)

    if (groupsContainingOwner.length === 0) {
        logger.info(`Owner '${owner}' is not included in any group; no owner updates will be performed.`)
    } else {
        logger.warn(
            `Owner '${owner}' appears in groups: [${groupsContainingOwner.join(", ")}]`.concat(
                " Proceeding with owner updates per config."
            )
        )
    }
}

const warnAboutSharedTokens = () => {
    const tokenToUsers = new Map()
    USERS.forEach((token, username) => {
        if (!token) return
        const normalizedToken = token.trim()
        if (!normalizedToken) return
        const list = tokenToUsers.get(normalizedToken) || []
        list.push(username)
        tokenToUsers.set(normalizedToken, list)
    })

    tokenToUsers.forEach((usernames) => {
        if (usernames.length <= 1) return
        logger.warn(
            `Users ${usernames.join(", ")} share the same Plex token. Plex applies default stream selections per account, so updates for one profile will impact the others.`
        )
    })
}

if (config.skipInaccessibleItems) {
    logger.info("skipInaccessibleItems enabled: HTTP 403 responses will be skipped per user/item.")
}

// Create an Axios instance with increased timeout and keep-alive
const axiosInstance = axios.create({
    baseURL: config.plex_server_url,
    headers: {
        "X-Plex-Token": config.plex_owner_token,
    },
    timeout: 600000,
})

// Utility to handle error logging
const handleAxiosError = (context, error) => {
    if (error.response) {
        logger.error(`Error ${context}: ${error.response.status} - ${error.response.statusText}`)
    } else if (error.request) {
        logger.error(`Error ${context}: No response received.`)
    } else {
        logger.error(`Error ${context}: ${error.message}`)
    }
}

// Function to delay execution
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

// Function to parse user details from XML
const getUserDetailsFromXml = async (xml) => {
    const parser = new xml2js.Parser()
    try {
        const result = await parser.parseStringPromise(xml)
        const sharedServers = result.MediaContainer.SharedServer || []
        const extractedData = sharedServers.map((server) => {
            const username = server.$.username
            const accessToken = server.$.accessToken
            return { username, accessToken }
        })
        return extractedData
    } catch (error) {
        throw new Error(`Error parsing XML: ${error.message}`)
    }
}

// Fetch all users listed in filters
const fetchAllUsersListedInFilters = async () => {
    try {
        if (!config.plex_client_identifier) throw new Error("Client identifier not supplied in config")
        const response = await axios.get(
            `https://plex.tv/api/servers/${config.plex_client_identifier}/shared_servers`,
            {
                headers: {
                    "X-Plex-Token": config.plex_owner_token,
                    Accept: "application/json",
                },
            }
        )
        const filterUsernames = new Set(Object.values(config.groups).flat())
        const users = await getUserDetailsFromXml(response.data)
        users.forEach((user) => {
            if (filterUsernames.has(user.username) || filterUsernames.has("$ALL")) {
                USERS.set(user.username, user.accessToken)
            }
        })

        const managedUsers = config.managed_users
        if (managedUsers) {
            Object.keys(managedUsers).forEach((user) => {
                const token = managedUsers[user]
                if (user && token) {
                    USERS.set(user, token)
                }
            })
            logger.info(`Finished processing managed users`)
        }
        logger.info("Fetched and stored user details successfully.")
    } catch (error) {
        logger.warn(`Could not fetch users with access to server: ${error.message}`)
        return
    }
}

// Verify each token can access the API endpoint
const fetchUsersWithAccess = async (libraryName) => {
    const { id } = await fetchLibraryDetailsByName(libraryName)
    const usersWithAccess = new Map()
    const groups = config.filters[libraryName]

    for (const group in groups) {
        let usernames = config.groups[group]
        let users = []
        if (usernames.includes("$ALL")) {
            usernames = [...USERS.keys()]
        }
        for (const username of usernames) {
            const token = USERS.get(username)
            try {
                const response = await axios.get(`${config.plex_server_url}/library/sections/${id}`, {
                    headers: { "X-Plex-Token": token },
                })
                if (response.status !== 200) throw new Error(`Unexpected response status: ${response.status}`)
                logger.debug(
                    `Checking if user ${username} of group ${group} has access to library ${libraryName}... OK`
                )
                users.push(username)
            } catch (error) {
                logger.warn(
                    `User ${username} of group ${group} can't access library ${libraryName}. They will be skipped during updates. ${error.message}`
                )
            }
            await delay(100)
        }
        usersWithAccess.set(group, users)
    }
    return usersWithAccess
}

// Setup Cron Job
const setupCronJob = () => {
    if (config.dry_run || !config.partial_run_cron_expression) return
    if (!cronValidator.isValidCron(config.partial_run_cron_expression))
        throw new Error(`Invalid cron expression: ${config.partial_run_cron_expression}`)
    cron.schedule(config.partial_run_cron_expression, async () => {
        logger.info(`Running scheduled partial run at ${new Date().toISOString()}`)
        await performPartialRun()
    })
    logger.info("Cron job set up successfully")
}

// Fetch all libraries and map by ID
const fetchAllLibraries = async () => {
    try {
        const { data } = await axiosInstance.get("/library/sections").catch(async (error) => {
            logger.error(`Error fetching libraries: ${error.message}. Retrying in 30 sec...`)
            let res = error.response
            let attempt = 1
            await delay(30000)
            while (res.status !== 200 && attempt < 10) {
                await axiosInstance
                    .get("/library/sections")
                    .then((response) => (res = response))
                    .catch((error) => {
                        logger.error(
                            `Attempt ${attempt}/10 failed with error: ${error.message}. Retrying in 30 sec... `
                        )
                    })

                if (res.status === 200) return res

                attempt++
                await delay(30000)
            }
            logger.error(`All attempts failed. Verify connection to Plex before restarting. Shutting down.`)
            process.exit(1)
        })
        const libraries = data?.MediaContainer?.Directory || []

        for (const libraryName in config.filters) {
            const library = libraries.find((lib) => lib.title.toLowerCase() === libraryName.toLowerCase())
            if (!library) throw new Error(`Library '${libraryName}' not found in Plex response`)
            if (library.type !== "movie" && library.type !== "show")
                throw new Error(`Invalid library type '${library.type}'. Must be 'movie' or 'show'`)

            LIBRARIES.set(library.key, { name: library.title, type: library.type })
            logger.debug(`Mapped library: ${library.title} (ID: ${library.key}, Type: ${library.type})`)
        }

        logger.info("Fetched and mapped libraries")
    } catch (error) {
        handleAxiosError("fetching libraries", error)
    }
}

// Load last run timestamps from the file
const loadLastRunTimestamps = () => {
    if (fs.existsSync(timestampsFile)) {
        const data = fs.readFileSync(timestampsFile, "utf-8")
        return JSON.parse(data)
    }
    return {}
}

// Save the new last run timestamps to the file
const saveLastRunTimestamps = (timestamps) => {
    fs.writeFileSync(timestampsFile, JSON.stringify(timestamps, null, 2), "utf-8")
}

// Fetch media items that were updated after a specific timestamp
const fetchUpdatedMediaItems = async (libraryId, lastUpdatedAt) => {
    try {
        const { data } = await axiosInstance.get(`/library/sections/${libraryId}/all`)
        const items = data?.MediaContainer?.Metadata || []

        // Filter items updated after the last known updatedAt timestamp
        return items.filter((item) => item.updatedAt > lastUpdatedAt)
    } catch (error) {
        handleAxiosError(`fetching updated media for Library ID ${libraryId}`, error)
        return []
    }
}

const evaluateStreams = (streams, filters) => {
    for (const filter of Object.values(filters)) {
        const { include, exclude } = filter

        const defaultStream = streams.find((stream) => {
            // Check 'include' first
            if (
                include &&
                Object.entries(include).some(([field, value]) => {
                    const streamValue = stream[field]?.toString().toLowerCase()
                    if (!streamValue) return true
                    const valuesArray = Array.isArray(value) ? value : [value]
                    return valuesArray.some((value) => !streamValue.includes(value.toString().toLowerCase()))
                })
            ) {
                return false
            }

            // Check 'exclude'
            if (
                exclude &&
                Object.entries(exclude).some(([field, value]) => {
                    const streamValue = stream[field]?.toString().toLowerCase()
                    if (!streamValue) return false
                    const valuesArray = Array.isArray(value) ? value : [value]
                    return valuesArray.some((value) => streamValue.includes(value.toString().toLowerCase()))
                })
            ) {
                return false
            }

            return true
        })

        if (defaultStream)
            return {
                id: defaultStream.id,
                extendedDisplayTitle: defaultStream.extendedDisplayTitle,
                displayTitle: defaultStream.displayTitle,
                language: defaultStream.language,
                codec: defaultStream.codec,
                onMatch: filter.on_match || {},
            }
    }
}

// Fetch streams for a specific media item (movie or episode)
const fetchStreamsForItem = async (itemId) => {
    try {
        const { data } = await axiosInstance.get(`/library/metadata/${itemId}`)
        const metadata = data?.MediaContainer?.Metadata[0]
        var title = metadata?.title
        if (metadata?.type == 'episode') {
          title = `Episode ${metadata.index} - ${title}`
        }
        if (metadata?.parentTitle) {
          title = `${metadata?.parentTitle} - ${title}`
        }
        if (metadata?.grandparentTitle) {
          title = `${metadata?.grandparentTitle} - ${title}`
        }
        const part = data?.MediaContainer?.Metadata[0]?.Media[0]?.Part[0]
        if (!part || !part.id || !part.Stream) {
            logger.warn(`Item ID ${itemId} '${title}' has invalid media structure. Skipping.`)
            return { ratingKey: metadata?.ratingKey || itemId, partId: itemId, title: title || 'title unknown', streams: [] }
        }
        const streams = part.Stream.filter((stream) => stream.streamType !== STREAM_TYPES.video)
        return { ratingKey: metadata?.ratingKey || itemId, partId: part.id, title: title, streams: streams }
    } catch (error) {
        handleAxiosError(`fetching streams for Item ID ${itemId}`, error)
        return { ratingKey: itemId, partId: itemId, title: 'title unknown', streams: [] } // Return empty streams on error
    }
}

// Fetch all episodes of a season
const fetchStreamsForSeason = async (seasonId) => {
    try {
        const { data } = await axiosInstance.get(`/library/metadata/${seasonId}/children`)
        const episodes = data?.MediaContainer?.Metadata || []
        if (episodes.length === 0) {
            logger.info(`No episodes found for '${data.parentTitle}' '${data.title}': Season ID ${seasonId}`)
            return []
        }
        // Fetch streams for each episode sequentially
        const streams = []
        for (const episode of episodes) {
            logger.debug(`Fetching '${episode.grandparentTitle}' ${episode.parentTitle} Episode ${episode.index}: '${episode.title}' streams`)
            const stream = await fetchStreamsForItem(episode.ratingKey)
            streams.push(stream)
            // Optional: Delay between fetching each episode to reduce load
            await delay(100)
        }
        return streams
    } catch (error) {
        handleAxiosError(`fetching episodes for Season ID ${seasonId}`, error)
        return []
    }
}

// Fetch all seasons of a show
const fetchStreamsForShow = async (showId) => {
    try {
        const { data } = await axiosInstance.get(`/library/metadata/${showId}/children`)
        const seasons = data?.MediaContainer?.Metadata || []
        if (seasons.length === 0) {
            logger.warn(`No seasons found for Show ID ${showId}: '${data.title}'`)
            return []
        }
        const streams = []
        for (const season of seasons) {
            logger.debug(`Fetching '${season.parentTitle}' Season '${season.index}' streams`)
            const seasonStreams = await fetchStreamsForSeason(season.ratingKey)
            streams.push(...seasonStreams)
            // Optional: Delay between fetching each season to reduce load
            await delay(100) // 100ms delay
        }
        return streams
    } catch (error) {
        handleAxiosError(`fetching seasons for Show ID ${showId}`, error)
        return []
    }
}

// Fetch library details by its name
const fetchLibraryDetailsByName = async (libraryName) => {
    try {
        for (const [key, details] of LIBRARIES.entries()) {
            if (details.name.toLowerCase() === libraryName.toLowerCase()) {
                return { id: key, type: details.type }
            }
        }
        throw new Error(`Library '${libraryName}' not found`)
    } catch (error) {
        handleAxiosError(`fetching library details for '${libraryName}'`, error)
        return { id: null, type: null }
    }
}

const findMatchingAudioStream = (part, audioFilters) => {
    if (!audioFilters) return

    const audioStreams = part.streams.filter((stream) => stream.streamType === STREAM_TYPES.audio)
    return evaluateStreams(audioStreams, audioFilters)
}

const findMatchingSubtitleStream = (part, subtitleFilters) => {
    if (!subtitleFilters) return
    if (subtitleFilters === "disabled") return { id: 0 }

    const subtitleStreams = part.streams.filter((stream) => stream.streamType === STREAM_TYPES.subtitles)
    return evaluateStreams(subtitleStreams, subtitleFilters)
}

// Determine which streams should be updated based on filters
const identifyStreamsToUpdate = async (parts, filters) => {
    try {
        const streamsToUpdate = []

        for (const part of parts) {
            if (!part.streams || part.streams.length <= 1) {
                logger.info(`Part ID ${part.partId} ('${part.title}') has only one stream. Skipping.`)
                continue
            }

            const partUpdate = { partId: part.partId, title: part.title, ratingKey: part.ratingKey }

            const audioStreams = part.streams.filter((stream) => stream.streamType === STREAM_TYPES.audio)
            const subtitleStreams = part.streams.filter((stream) => stream.streamType === STREAM_TYPES.subtitles)

            const selectedAudio = audioStreams.find((stream) => stream.selected === 1 || stream.selected === "1" || stream.selected === true)
            if (selectedAudio) {
                partUpdate.fromAudioStreamId = selectedAudio.id
                partUpdate.fromAudioStreamLabel =
                    selectedAudio.extendedDisplayTitle || selectedAudio.displayTitle || selectedAudio.language || "unknown"
            }

            const selectedSubtitle = subtitleStreams.find(
                (stream) => stream.selected === 1 || stream.selected === "1" || stream.selected === true
            )
            if (selectedSubtitle) {
                partUpdate.fromSubtitleStreamId = selectedSubtitle.id
                partUpdate.fromSubtitleStreamLabel =
                    selectedSubtitle.extendedDisplayTitle ||
                    selectedSubtitle.displayTitle ||
                    selectedSubtitle.language ||
                    "unknown"
            }

            let audio = findMatchingAudioStream(part, filters.audio) || {}
            let subtitles = findMatchingSubtitleStream(part, filters.subtitles) || {}

            if (audio?.onMatch?.subtitles) {
                subtitles = findMatchingSubtitleStream(part, audio.onMatch.subtitles)
            }

            if (subtitles?.onMatch?.audio) {
                audio = findMatchingAudioStream(part, subtitles.filter.onMatch.audio)
            }

            if (audio.id) {
                partUpdate.audioStreamId = audio.id
                partUpdate.audioStreamLabel = audio.extendedDisplayTitle || audio.displayTitle || audio.language || audio.codec
                logger.info(`Part ID ${part.partId} ('${part.title}'): match found for audio stream ${audio.extendedDisplayTitle}`)
            } else {
                logger.debug(`Part ID ${part.partId} ('${part.title}'): no match found for audio streams`)
            }
            if (subtitles.id >= 0) {
                partUpdate.subtitleStreamId = subtitles.id
                partUpdate.subtitleStreamLabel =
                    subtitles.id === 0
                        ? "Disabled"
                        : subtitles.extendedDisplayTitle || subtitles.displayTitle || subtitles.language || subtitles.codec
                logger.info(
                    `Part ID ${part.partId} ('${part.title}'): ${
                        subtitles.id === 0
                            ? "subtitles disabled"
                            : `match found for subtitle stream ${subtitles.extendedDisplayTitle}`
                    }`
                )
            } else {
                logger.debug(`Part ID ${part.partId} ('${part.title}'): no match found for subtitle streams`)
            }

            if (partUpdate.audioStreamId || partUpdate.subtitleStreamId >= 0) {
                streamsToUpdate.push(partUpdate)
            }
        }
        return streamsToUpdate
    } catch (error) {
        logger.error(`Error while evaluating streams for filter: ${error.message}. Aborting`)
        return []
    }
}

// Update default streams for a single item across all relevant users
const maskToken = (token) => {
    if (!token) return "(none)"
    const normalized = token.toString().trim()
    if (normalized.length <= 6) return "*".repeat(normalized.length)
    return `${"*".repeat(normalized.length - 6)}${normalized.slice(-6)}`
}

const auditWriter = new AuditWriter(config.auditDir)
let auditWriterClosed = false
const closeAuditWriter = () => {
    if (auditWriterClosed) return
    auditWriter.close()
    auditWriterClosed = true
}

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

const emitUserSummary = (summary) => {
    if (!config.logUserSummary && !config.logJsonUserSummary) return

    if (config.logJsonUserSummary) {
        logger.info(
            JSON.stringify({
                event: "user_updates",
                group: summary.group,
                partId: summary.partId,
                title: summary.title,
                library: summary.libraryName,
                users: summary.users.map((user) => {
                    const payload = { name: user.name, status: user.status }
                    if (user.reason) payload.reason = user.reason
                    if (user.httpStatus) payload.httpStatus = user.httpStatus
                    if (user.audio) payload.audio = user.audio
                    if (user.subtitles) payload.subtitles = user.subtitles
                    return payload
                }),
            })
        )
    }

    if (config.logUserSummary) {
        const header = `User updates (group=${summary.group}, part=${summary.partId}, title='${summary.title}'):`
        const lines = summary.users.map((user) => {
            const pieces = [`${user.name}:`]
            if (user.audio) pieces.push(`audio='${user.audio.label}' (id=${user.audio.id})`)
            if (user.subtitles)
                pieces.push(
                    user.subtitles.id === 0
                        ? "subtitles=disabled"
                        : `subtitles='${user.subtitles.label}' (id=${user.subtitles.id})`
                )
            pieces.push(
                user.status === "success"
                    ? "[success]"
                    : user.status === "skipped"
                    ? `[skipped: ${user.reason || "unknown"}]`
                    : `[error: ${user.reason || "unknown"}]`
            )
            return `  ${pieces.join(" ")}`
        })
        logger.info(`${header}\n${lines.join("\n")}`)
    }
}

const updateDefaultStreamsPerItem = async (libraryName, streamsToUpdate, users) => {
    for (const group in streamsToUpdate) {
        for (const stream of streamsToUpdate[group]) {
            const usernames = users.get(group)
            if (!usernames || usernames.length === 0) {
                logger.warn(`No users found in group '${group}'. Skipping update.`)
                continue
            }

            const summary = {
                libraryName,
                group,
                partId: stream.partId,
                title: stream.title,
                users: [],
            }

            const queryParams = new URLSearchParams()
            if (stream.audioStreamId) queryParams.append("audioStreamID", stream.audioStreamId)
            if (stream.subtitleStreamId >= 0) queryParams.append("subtitleStreamID", stream.subtitleStreamId)

            for (const username of usernames) {
                const token = USERS.get(username)
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
                    const transition = buildStreamTransition(stream)
                    auditWriter.append({
                        timestamp: new Date().toISOString(),
                        libraryName,
                        ratingKey: stream.ratingKey,
                        partId: stream.partId,
                        title: stream.title,
                        group,
                        user: username,
                        actionType: describeActionType(stream),
                        ...transition,
                        status: "skipped",
                        reason: "no_token",
                    })
                    continue
                }

                const userClient = axios.create({
                    baseURL: config.plex_server_url,
                    timeout: 600000,
                    headers: {
                        "X-Plex-Token": token,
                        "X-Plex-Client-Identifier": config.plex_client_identifier,
                        "X-Plex-Device-Name": `Defaulter/${username}`,
                        ...(username ? { "X-Plex-Username": username } : {}),
                    },
                })

                runStats.processed++
                const startTime = Date.now()
                let skipUpdate = false
                let httpStatus
                let reason

                let response
                let attempt = 0
                let fatalError = false
                while (attempt < 10 && !response && !skipUpdate) {
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
                        httpStatus = error.response?.status
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
                            if (attempt < 9) {
                                logger.error(
                                    `Attempt ${attempt + 1}/10 failed for user ${username} in group ${group}${messageSuffix}: ${reason}. Retrying in 30 sec...`
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
                const transition = buildStreamTransition(stream)
                auditWriter.append({
                    timestamp: new Date(startTime).toISOString(),
                    libraryName,
                    ratingKey: stream.ratingKey,
                    partId: stream.partId,
                    title: stream.title,
                    group,
                    user: username,
                    actionType: describeActionType(stream),
                    ...transition,
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
                const subtitleMessage =
                    stream.subtitleStreamId >= 0 ? `Subtitle ID ${stream.subtitleStreamId}` : ""
                const updateMessage = [audioMessage, subtitleMessage].filter(Boolean).join(" and ")
                logger.debug(
                    `Update ${updateMessage} for user ${username} in group ${group}: ${
                        statusForLog === 200 ? "SUCCESS" : "FAIL"
                    }`
                )

                summary.users.push(userSummary)
                if (fatalError) {
                    emitUserSummary(summary)
                    closeAuditWriter()
                    process.exit(1)
                }
                await delay(100)
            }

            logger.info(`Part ID ${stream.partId}: update complete for group ${group}`)
            emitUserSummary(summary)
        }
    }
}

// Identify streams for dry run
const identifyStreamsForDryRun = async () => {
    for (const libraryName in config.filters) {
        logger.info(`Processing library for dry run: ${libraryName}`)
        const { id, type } = await fetchLibraryDetailsByName(libraryName)
        if (!id || !type) {
            logger.warn(`Library '${libraryName}' details are incomplete. Skipping.`)
            continue
        }
        const updatedItems = await fetchUpdatedMediaItems(id, 0)

        if (type === "movie") {
            for (const item of updatedItems) {
                const { title, ratingKey } = item;
                logger.info(`Fetching streams for ${type} '${title}'`)
                const stream = await fetchStreamsForItem(ratingKey)
                const groupFilters = config.filters[libraryName]
                const newStreams = {}

                for (const group in groupFilters) {
                    const matchedStreams = await identifyStreamsToUpdate([stream], groupFilters[group])
                    if (matchedStreams.length > 0) {
                        newStreams[group] = matchedStreams
                    }
                }
                await delay(100)
            }
        } else if (type === "show") {
            for (const item of updatedItems) {
                const { title, ratingKey } = item;
                logger.info(`Fetching streams for ${type} '${title}'`)
                const showStreams = await fetchStreamsForShow(ratingKey)
                for (const stream of showStreams) {
                    const groupFilters = config.filters[libraryName]
                    const newStreams = {}

                    for (const group in groupFilters) {
                        const matchedStreams = await identifyStreamsToUpdate([stream], groupFilters[group])
                        if (matchedStreams.length > 0) {
                            newStreams[group] = matchedStreams
                        }
                    }
                    await delay(100)
                }
            }
        }
    }
}

// Dry run to identify streams without applying updates
const performDryRun = async () => {
    await fetchAllLibraries()
    logger.info("STARTING DRY RUN. NO CHANGES WILL BE MADE.")
    await identifyStreamsForDryRun()
    logger.info("DRY RUN COMPLETE.")
}

// Partial run: process items updated since last run
const performPartialRun = async (cleanRun) => {
    await fetchAllLibraries()
    resetRunStats()

    logger.info(`STARTING ${cleanRun ? "CLEAN" : "PARTIAL"} RUN`)

    const lastRunTimestamps = cleanRun ? {} : loadLastRunTimestamps()
    const newTimestamps = {}

    for (const libraryName in config.filters) {
        logger.info(`Processing library: ${libraryName}`)
        const { id, type } = await fetchLibraryDetailsByName(libraryName)
        if (!id || !type) {
            logger.warn(`Library '${libraryName}' details are incomplete. Skipping.`)
            continue
        }
        const lastUpdatedAt = lastRunTimestamps[libraryName] || 0

        // Fetch updated media items based on updatedAt timestamp
        const updatedItems = await fetchUpdatedMediaItems(id, lastUpdatedAt)
        if (!updatedItems || updatedItems.length === 0) {
            logger.info(`No changes detected in library ${libraryName} since the last run`)
            continue
        }

        const usersWithAccess = await fetchUsersWithAccess(libraryName)
        if (![...usersWithAccess.values()].some((users) => users.length > 0)) {
            logger.warn(`No users have access to library ${libraryName}. Skipping`)
            continue
        }

        if (type === "movie") {
            for (const item of updatedItems) {
                const stream = await fetchStreamsForItem(item.ratingKey)
                const groupFilters = config.filters[libraryName]
                const newStreams = {}

                for (const group in groupFilters) {
                    const matchedStreams = await identifyStreamsToUpdate([stream], groupFilters[group])
                    if (matchedStreams.length > 0) {
                        newStreams[group] = matchedStreams
                    }
                }

                if (Object.keys(newStreams).length > 0) {
                    await updateDefaultStreamsPerItem(libraryName, newStreams, usersWithAccess)
                }

                // Optional: Delay between processing each item to reduce load
                await delay(100) // 100ms delay
            }
        } else if (type === "show") {
            for (const item of updatedItems) {
                const showStreams = await fetchStreamsForShow(item.ratingKey)
                for (const stream of showStreams) {
                    const groupFilters = config.filters[libraryName]
                    const newStreams = {}

                    for (const group in groupFilters) {
                        const matchedStreams = await identifyStreamsToUpdate([stream], groupFilters[group])
                        if (matchedStreams.length > 0) {
                            newStreams[group] = matchedStreams
                        }
                    }

                    if (Object.keys(newStreams).length > 0) {
                        await updateDefaultStreamsPerItem(libraryName, newStreams, usersWithAccess)
                    }

                    // Optional: Delay between processing each stream to reduce load
                    await delay(100) // 100ms delay
                }
            }
        }

        // Update the timestamp for the current library
        const latestUpdatedAt = Math.max(...updatedItems.map((item) => item.updatedAt))
        newTimestamps[libraryName] = latestUpdatedAt
    }

    // Save the updated timestamps for future runs
    if (Object.keys(newTimestamps).length > 0) saveLastRunTimestamps({ ...lastRunTimestamps, ...newTimestamps })

    logRunSummary()
    logger.info(`FINISHED ${cleanRun ? "CLEAN" : "PARTIAL"} RUN`)
}

// Tautulli webhook for new items
app.post("/webhook", async (req, res) => {
    try {
        logger.info("Tautulli webhook received. Processing...")

        const { type, libraryId, mediaId } = req.body
        if (!type || !libraryId || !mediaId) throw new Error("Error getting request body")

        let libraryName = LIBRARIES.get(libraryId)?.name
        if (!libraryName) {
            // This only triggers if something goes wrong in Tautulli/Plex. Quick refresh should fix it.
            logger.info(`Library ID ${libraryId} not found in filters. Attempting library refresh...`)
            await fetchAllLibraries()

            libraryName = LIBRARIES.get(libraryId)?.name
            if (!libraryName) {
                logger.info(`Library ID ${libraryId} not found in filters. Ending request`)
                return res.status(200).send("Event not relevant")
            }
        }

        const usersWithAccess = await fetchUsersWithAccess(libraryName)
        const filters = config.filters[libraryName]

        let streams = [] // Need arrays for identifyStreamsToUpdate
        if (type === "movie" || type === "episode") {
            streams = [await fetchStreamsForItem(mediaId)]
        } else if (type === "show") {
            streams = await fetchStreamsForShow(mediaId)
        } else if (type === "season") {
            streams = await fetchStreamsForSeason(mediaId)
        }
        // else do nothing

        const updates = {}
        for (const group in filters) {
            const newStreams = await identifyStreamsToUpdate(streams, filters[group])
            if (!newStreams || newStreams.length === 0) {
                logger.info("Could not find streams to update. Ending request")
                continue
            }
            updates[group] = newStreams
        }

        if (Object.keys(updates).length > 0) {
            await updateDefaultStreamsPerItem(libraryName, updates, usersWithAccess)
        }

        logger.info("Tautulli webhook finished")
        return res.status(200).send("Webhook received and processed.")
    } catch (error) {
        logger.error(`Error processing webhook: ${error.message}`)
        res.status(500).send("Error processing webhook")
    }
})

// Handle uncaught exceptions and unhandled rejections
process.on("uncaughtException", (error) => {
    logger.error(`Uncaught exception: ${error.message}`)
    closeAuditWriter()
})
process.on("unhandledRejection", (reason) => {
    logger.error(`Unhandled rejection: ${reason}`)
    closeAuditWriter()
})
process.on("SIGINT", () => {
    closeAuditWriter()
    process.exit(1)
})
process.on("SIGTERM", () => {
    closeAuditWriter()
    process.exit(1)
})
process.on("exit", () => {
    closeAuditWriter()
})

// Initializing the application
const PORT = process.env.PORT || 3184
app.listen(PORT, async () => {
    logger.info(`Server is running on port ${PORT}`)
    try {
        if (config.plex_owner_name) {
            USERS.set(config.plex_owner_name, config.plex_owner_token)
        }
        await fetchAllUsersListedInFilters()
        if (USERS.size === 0) throw new Error("No users with access to libraries detected")

        warnAboutSharedTokens()
        logGroupDigest()
        logOwnerSafety()

        if (config.dry_run) await performDryRun()
        else if (config.partial_run_on_start) await performPartialRun()
        else if (config.clean_run_on_start) await performPartialRun(config.clean_run_on_start)
        else await fetchAllLibraries()

        setupCronJob()
    } catch (error) {
        logger.error(`Error initializing the application: ${error.message}`)
        process.exit(1)
    }
})
