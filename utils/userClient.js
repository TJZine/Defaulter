const axios = require("axios")

const DEFAULT_TIMEOUT = 600000

const buildDeviceName = (username, prefix = "Defaulter") => {
    const safeUsername = username || "unknown"
    return `${prefix}/${safeUsername}`
}

const buildUserHeaders = ({ token, username, clientIdentifier, deviceNamePrefix = "Defaulter" }) => {
    if (!token) throw new Error("token is required to build headers")
    const headers = {
        "X-Plex-Token": token,
        "X-Plex-Client-Identifier": clientIdentifier,
        "X-Plex-Device-Name": buildDeviceName(username, deviceNamePrefix),
    }
    if (username) headers["X-Plex-Username"] = username
    return headers
}

const createUserClient = (
    { baseURL, clientIdentifier, deviceNamePrefix = "Defaulter", timeout = DEFAULT_TIMEOUT, axiosLib = axios },
    { username, token }
) => {
    const headers = buildUserHeaders({ token, username, clientIdentifier, deviceNamePrefix })
    return axiosLib.create({ baseURL, timeout, headers })
}

module.exports = {
    buildUserHeaders,
    createUserClient,
}
