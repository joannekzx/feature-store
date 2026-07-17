package com.featurestore.flink;

import com.fasterxml.jackson.databind.JsonNode;

// the data contract every incoming event has to honour before it's allowed to become a feature.
// field names, types, and the schema version it was written against all get checked here.
// the point: if an upstream team renames or retypes a field, we want it caught at the door and
// not silently landing as a default value (a renamed "amount" quietly becomes 0.0) that then
// corrupts Redis and the Parquet training history.
public final class FeatureSchema {

    // bump this whenever the contract below changes. producers stamp the same number onto every
    // event, so anything written against a version we don't recognise gets rejected instead of guessed at.
    public static final int CURRENT_VERSION = 1;

    private FeatureSchema() {}

    // checks one raw event against v(CURRENT_VERSION). returns null if it's valid,
    // otherwise a short reason so the rejection is readable in the log.
    static String validate(JsonNode e) {
        if (!e.hasNonNull("schemaVersion") || !e.get("schemaVersion").isInt())
            return "missing/invalid schemaVersion";
        if (e.get("schemaVersion").asInt() != CURRENT_VERSION)
            return "unknown schemaVersion " + e.get("schemaVersion").asInt() + " (this job speaks v" + CURRENT_VERSION + ")";
        if (!e.hasNonNull("entityId") || !e.get("entityId").isTextual())
            return "missing/non-string entityId";
        if (!e.hasNonNull("amount") || !e.get("amount").isNumber())
            return "missing/non-numeric amount";   // e.g. upstream renamed amount -> amt
        if (!e.hasNonNull("eventType") || !e.get("eventType").isInt())
            return "missing/non-int eventType";     // e.g. upstream changed the type int -> string
        if (!e.hasNonNull("timestamp") || !e.get("timestamp").isNumber())
            return "missing/non-numeric timestamp";
        return null;
    }
}
