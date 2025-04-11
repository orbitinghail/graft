"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _utils = require("./utils/index.cjs");
var _mongodb = require("mongodb");
const DRIVER_NAME = "mongodb";
module.exports = (0, _utils.defineDriver)(opts => {
  let collection;
  const getMongoCollection = () => {
    if (!collection) {
      if (!opts.connectionString) {
        throw (0, _utils.createRequiredError)(DRIVER_NAME, "connectionString");
      }
      const mongoClient = new _mongodb.MongoClient(opts.connectionString);
      const db = mongoClient.db(opts.databaseName || "unstorage");
      collection = db.collection(opts.collectionName || "unstorage");
    }
    return collection;
  };
  return {
    name: DRIVER_NAME,
    options: opts,
    getInstance: getMongoCollection,
    async hasItem(key) {
      const result = await getMongoCollection().findOne({
        key
      });
      return !!result;
    },
    async getItem(key) {
      const document = await getMongoCollection().findOne({
        key
      });
      return document?.value ?? null;
    },
    async setItem(key, value) {
      const currentDateTime = /* @__PURE__ */new Date();
      await getMongoCollection().findOneAndUpdate({
        key
      }, {
        $set: {
          key,
          value,
          modifiedAt: currentDateTime
        },
        $setOnInsert: {
          createdAt: currentDateTime
        }
      }, {
        upsert: true,
        returnDocument: "after"
      });
      return;
    },
    async removeItem(key) {
      await getMongoCollection().deleteOne({
        key
      });
    },
    async getKeys() {
      return await getMongoCollection().find().project({
        key: true
      }).map(d => d.key).toArray();
    },
    async getMeta(key) {
      const document = await getMongoCollection().findOne({
        key
      });
      return document ? {
        mtime: document.modifiedAt,
        birthtime: document.createdAt
      } : {};
    },
    async clear() {
      await getMongoCollection().deleteMany({});
    }
  };
});