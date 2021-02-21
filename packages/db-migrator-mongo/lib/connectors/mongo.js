const { BaseConnector } = require('../../../db-migrator-core');
const MongoClient = require('mongodb').MongoClient;
const errors = require('../errors');

class MongoConnecter extends BaseConnector {

  validateConfig(config) {
    return config.hasOwnProperty('url') && config.hasOwnProperty('database');
  }

  connectDatabase(config) {
    return new Promise((resolve, reject) => {
      if (!this.validateConfig(config)) return reject('INVALID_DATABASE_CONFIG');
      MongoClient.connect(config.url, {
        useUnifiedTopology: true,
      }).then((client) => {
        this.database = client.db(config.database);
        this.config = config;
        console.log(`DbMigrator :: Database :: connection successful :: ${config.database}`);
        resolve();
      }).catch((error) => Promise.reject(error));
    });
  }

  validateSourceMigrator(from) {
    const errorsArray = [];
    const { collection, query = {}, aggregate = [] } = from;
    if (!collection) errorsArray.push(errors.invalidFromCollection());
    if (typeof query !== 'object') errorsArray.push(errors.invalidFromQuery());
    if (!Array.isArray(aggregate)) errorsArray.push(errors.invalidFromAggregate());
    return errorsArray;
  }

  validateTargetMigrator(to) {
    const errorsArray = [];
    const { collection } = to;
    if (!collection) errorsArray.push(errors.invalidToCollection());
    return errorsArray;    
  }

  // Return documents count as per the query provided
  // as of now, this does not work as per aggregate
  countDocuments(from) {
    const { collection, query = {}, aggregate = [] } = from;
    if (aggregate && Array.isArray(aggregate) && aggregate.length) {
      return this.countDocumentsAggregate(collection, aggregate);
    }
    if (!aggregate || !aggregate.length) {
      return this.countDocumentsQuery(collection, query);
    }
  }

  // fetch the documents as per the query/aggregate, limit, skip etc
  fetchDocuments(from, skip, limit, cb) {
    const { collection, query = {}, aggregate = [] } = from;
    if (aggregate && Array.isArray(aggregate) && aggregate.length) {
      return this.fetchDocumentsAggregate(collection, aggregate, skip, limit, cb);
    }
    if (!aggregate || !aggregate.length) {
      return this.fetchDocumentsQuery(collection, query, skip, limit, cb);
    }
  }

  // Save document to target collection in target database
  storeDocument(to, document) {
    return this.database.collection(to.collection).insertOne(document);
  }

  countDocumentsQuery(collection, query) {
    return this.database.collection(collection).countDocuments(query);
  }

  countDocumentsAggregate(collection, aggregate) {
    return new Promise((resolve, reject) => {
      const index = aggregate.findIndex((param) => Object.prototype.hasOwnProperty.call(param, '$count'));
      if (index === -1) {
        aggregate.push({ $count: 'count' });
      } else {
        aggregate[index] = { $count: 'count' };
      }

      this.database.collection(collection).aggregate(aggregate).toArray((err, res) => {
        if (err) return reject(err);
        resolve(res.length ? res[0].count : 0);
      });
    });
  }

  fetchDocumentsQuery(collection, query, skip, limit, cb) {
    return this.database.collection(collection).find(query).limit(limit).skip(skip)
      .toArray((err, documents) => cb(err, documents));
  }

  fetchDocumentsAggregate(collection, aggregate, skip, limit, cb) {
    // Work for aggregate in migrator config
    let index = aggregate.findIndex((param) => Object.prototype.hasOwnProperty.call(param, '$skip'));
    if (index === -1) {
      aggregate.push({ $skip: skip });
    } else {
      aggregate[index] = { $skip: skip };
    }

    index = aggregate.findIndex((param) => Object.prototype.hasOwnProperty.call(param, '$limit'));
    if (index === -1) {
      aggregate.push({ $limit: limit });
    } else {
      aggregate[index] = { $limit: limit };
    }

    this.database.collection(collection).aggregate(aggregate).toArray(
      (err, documents) => cb(err, documents),
    );

  }


}

module.exports = MongoConnecter;
