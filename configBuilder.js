const fs = require("fs")
const yaml = require("js-yaml")
const logger = require("./logger")
const cliOptions = require("./cliOptions")
const Ajv = require("ajv")
const ajv = new Ajv()

const yamlFilePath = cliOptions.getFlag("config") || cliOptions.positional[1] || "./config.yaml"

// Define the updated validation schema
const schema = {
    type: "object",
    properties: {
        plex_server_url: { type: "string", minLength: 1 },
        plex_owner_name: { type: "string" },
        plex_owner_token: { type: "string", minLength: 1 },
        plex_client_identifier: { type: "string", minLength: 1 },
        dry_run: { type: "boolean" },
        partial_run_on_start: { type: "boolean" },
        partial_run_cron_expression: { type: "string" },
        clean_run_on_start: { type: "boolean" },
        skipInaccessibleItems: { type: "boolean" },
        managed_users: {
            type: "object",
            additionalProperties: { type: "string" },
        },
        logUserSummary: { type: "boolean" },
        logJsonUserSummary: { type: "boolean" },
        auditDir: { type: "string" },
        groups: {
            type: "object",
            patternProperties: {
                ".*": {
                    type: "array",
                    items: { type: "string" },
                },
            },
            additionalProperties: false,
        },
        filters: {
            type: "object",
            patternProperties: {
                ".*": {
                    type: "object",
                    patternProperties: {
                        ".*": {
                            type: "object",
                            properties: {
                                audio: {
                                    type: "array",
                                    items: {
                                        type: "object",
                                        properties: {
                                            include: {
                                                type: "object",
                                                additionalProperties: {
                                                    oneOf: [
                                                        { type: "string" }, // Single value
                                                        {
                                                            type: "array",
                                                            items: { type: "string" }, // Array of values
                                                        },
                                                    ],
                                                },
                                            },
                                            exclude: {
                                                type: "object",
                                                additionalProperties: {
                                                    oneOf: [
                                                        { type: "string" }, // Single value
                                                        {
                                                            type: "array",
                                                            items: { type: "string" }, // Array of values
                                                        },
                                                    ],
                                                },
                                            },
                                            on_match: {
                                                type: "object",
                                                properties: {
                                                    subtitles: {
                                                        oneOf: [
                                                            { type: "string", enum: ["disabled"] },
                                                            {
                                                                type: "array",
                                                                items: {
                                                                    type: "object",
                                                                    properties: {
                                                                        include: {
                                                                            type: "object",
                                                                            additionalProperties: {
                                                                                oneOf: [
                                                                                    { type: "string" }, // Single value
                                                                                    {
                                                                                        type: "array",
                                                                                        items: { type: "string" }, // Array of values
                                                                                    },
                                                                                ],
                                                                            },
                                                                        },
                                                                        exclude: {
                                                                            type: "object",
                                                                            additionalProperties: {
                                                                                oneOf: [
                                                                                    { type: "string" }, // Single value
                                                                                    {
                                                                                        type: "array",
                                                                                        items: { type: "string" }, // Array of values
                                                                                    },
                                                                                ],
                                                                            },
                                                                        },
                                                                    },
                                                                    additionalProperties: false,
                                                                },
                                                            },
                                                        ],
                                                    },
                                                },
                                                additionalProperties: false,
                                            },
                                        },
                                        additionalProperties: false,
                                    },
                                },
                                subtitles: {
                                    oneOf: [
                                        { type: "string", enum: ["disabled"] },
                                        {
                                            type: "array",
                                            items: {
                                                type: "object",
                                                properties: {
                                                    include: {
                                                        type: "object",
                                                        additionalProperties: {
                                                            oneOf: [
                                                                { type: "string" }, // Single value
                                                                {
                                                                    type: "array",
                                                                    items: { type: "string" }, // Array of values
                                                                },
                                                            ],
                                                        },
                                                    },
                                                    exclude: {
                                                        type: "object",
                                                        additionalProperties: {
                                                            oneOf: [
                                                                { type: "string" }, // Single value
                                                                {
                                                                    type: "array",
                                                                    items: { type: "string" }, // Array of values
                                                                },
                                                            ],
                                                        },
                                                    },
                                                    on_match: {
                                                        type: "object",
                                                        properties: {
                                                            audio: {
                                                                oneOf: [
                                                                    { type: "string", enum: ["disabled"] },
                                                                    {
                                                                        type: "array",
                                                                        items: {
                                                                            type: "object",
                                                                            properties: {
                                                                                include: {
                                                                                    type: "object",
                                                                                    additionalProperties: {
                                                                                        oneOf: [
                                                                                            { type: "string" }, // Single value
                                                                                            {
                                                                                                type: "array",
                                                                                                items: {
                                                                                                    type: "string",
                                                                                                }, // Array of values
                                                                                            },
                                                                                        ],
                                                                                    },
                                                                                },
                                                                                exclude: {
                                                                                    type: "object",
                                                                                    additionalProperties: {
                                                                                        oneOf: [
                                                                                            { type: "string" }, // Single value
                                                                                            {
                                                                                                type: "array",
                                                                                                items: {
                                                                                                    type: "string",
                                                                                                }, // Array of values
                                                                                            },
                                                                                        ],
                                                                                    },
                                                                                },
                                                                            },
                                                                            additionalProperties: false,
                                                                        },
                                                                    },
                                                                ],
                                                            },
                                                        },
                                                        additionalProperties: false,
                                                    },
                                                },
                                                additionalProperties: false,
                                            },
                                        },
                                    ],
                                },
                            },
                            additionalProperties: false,
                        },
                    },
                },
            },
        },
    },
    required: ["plex_server_url", "plex_owner_token", "plex_client_identifier", "groups", "filters"],
    additionalProperties: false,
}

const formatErrors = (errors) => {
    return errors.map((error) => `"${error.instancePath}": ${error.message || "Validation error"}`).join("\n")
}

const normalizeUrl = (url) => (url.endsWith("/") ? url.slice(0, -1) : url)

// Function to load and validate YAML
const loadAndValidateYAML = () => {
    try {
        // Read and parse the YAML file
        const fileContent = fs.readFileSync(yamlFilePath, "utf8")
        const jsonData = yaml.load(fileContent)

        // Validate the JSON data against the schema
        const validate = ajv.compile(schema)
        const isValid = validate(jsonData)

        if (!isValid) throw new Error(`\n${formatErrors(validate.errors)}`)

        logger.info("Validated and loaded config file")
        jsonData.plex_server_url = normalizeUrl(jsonData.plex_server_url)
        if (typeof jsonData.skipInaccessibleItems !== "boolean") jsonData.skipInaccessibleItems = false
        return jsonData
    } catch (error) {
        logger.error(`Error loading or validating YAML: ${error.message}`)
        process.exit(1)
    }
}

module.exports = loadAndValidateYAML
