"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.processQueue = void 0;
const admin = require("firebase-admin");
const functions = require("firebase-functions");
const messagebird_1 = require("messagebird");
const config_1 = require("./config");
const log_1 = require("./log");
let db;
let mb;
let initialized = false;
/**
 * Initializes Admin SDK & MessageBird client if not already initialized.
 */
function initialize() {
    if (initialized === true)
        return;
    initialized = true;
    log_1.logInfo("initializing app...");
    admin.initializeApp();
    log_1.logInfo("initializing db...");
    db = admin.firestore();
    log_1.logInfo("initializing mb api client...");
    mb = messagebird_1.default(config_1.default.accessKey, undefined, ["ENABLE_FIREBASE_PLUGIN"]);
    log_1.logInfo("initialization finished successfully");
}
async function deliver(payload, ref) {
    const update = {
        "delivery.attempts": admin.firestore.FieldValue.increment(1),
        "delivery.endTime": admin.firestore.FieldValue.serverTimestamp(),
        "delivery.error": null,
        "delivery.leaseExpireTime": null,
    };
    log_1.logInfo("delivery attempt");
    try {
        // if (!payload.channelId) {
        //   throw new Error("Failed to deliver message. ChannelId is not defined.");
        // }
        if (!payload.to) {
            throw new Error("Failed to deliver message. Recipient of the message should be filled.");
        }
        if (!payload.content) {
            throw new Error("Failed to deliver message. Message content is empty.");
        }
        var params = {
            originator: "InfoSkills Technology",
            recipients: [`${payload.to.toString()}`],
            body: `${payload.content["text"]}`,
        };
        log_1.logInfo(`sending message to channelId: ${payload.channelId}`);
        log_1.logInfo(`with content:`, payload.content);
        await new Promise((resolve, reject) => {
            mb.messages.create(params, function (err, response) {
                if (err) {
                    log_1.logWarn(`send failed, got error: ${err}`);
                    return reject(err);
                }
                log_1.logInfo(`send successfully scheduled, got response: ${response}`);
                update["messageId"] = response.id;
                update["delivery.state"] = "SUCCESS";
                resolve();
            });
        });
    }
    catch (e) {
        log_1.logInfo(`updating delivery record with error message`);
        update["delivery.state"] = "ERROR";
        update["delivery.error"] = e.toString();
    }
    return db.runTransaction((transaction) => {
        transaction.update(ref, update);
        return Promise.resolve();
    });
}
async function processCreate(snap) {
    log_1.logInfo("new msg added, init delivery object for it");
    return db.runTransaction((transaction) => {
        transaction.update(snap.ref, {
            delivery: {
                startTime: admin.firestore.FieldValue.serverTimestamp(),
                state: "PENDING",
                attempts: 0,
                error: null,
            },
        });
        return Promise.resolve();
    });
}
async function processWrite(change) {
    log_1.logInfo("processing write");
    if (!change.after.exists) {
        log_1.logInfo("ignoring delete");
        return null;
    }
    if (!change.before.exists && change.after.exists) {
        log_1.logInfo("process create");
        return processCreate(change.after);
    }
    const payload = change.after.data();
    log_1.logInfo("processing update");
    switch (payload.delivery.state) {
        case "SUCCESS":
        case "ERROR":
            log_1.logInfo("current state is SUCCESS/ERROR");
            return null;
        case "PROCESSING":
            log_1.logInfo("current state is PROCESSING");
            if (payload.delivery.leaseExpireTime.toMillis() < Date.now()) {
                return db.runTransaction((transaction) => {
                    transaction.update(change.after.ref, {
                        "delivery.state": "ERROR",
                        error: "Message processing lease expired.",
                    });
                    return Promise.resolve();
                });
            }
            return null;
        case "PENDING":
        case "RETRY":
            log_1.logInfo("current state is PENDING/RETRY");
            await db.runTransaction((transaction) => {
                transaction.update(change.after.ref, {
                    "delivery.state": "PROCESSING",
                    "delivery.leaseExpireTime": admin.firestore.Timestamp.fromMillis(Date.now() + 60000),
                });
                return Promise.resolve();
            });
            log_1.logInfo("record set to PROCESSING state, trying to deliver the message");
            return deliver(payload, change.after.ref);
    }
}
exports.processQueue = functions.handler.firestore.document.onWrite(async (change) => {
    initialize();
    try {
        await processWrite(change);
    }
    catch (err) {
        log_1.logWarn("unexpected error during execution: ", err);
        return null;
    }
});
//# sourceMappingURL=index.js.map