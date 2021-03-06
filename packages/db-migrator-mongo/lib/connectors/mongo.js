const { BaseConnector } = require('../../../db-migrator-core');
const MongoClient = require('mongodb').MongoClient;
const errors = require('../errors');
const debug = require('debug')('db-migrator:mongo');

const ERROR_CODES = {
  11000: 'DUPLICATE_DOCUMENT',
}
class MongoConnecter extends BaseConnector {

  getDocumentError(err) {
    if (ERROR_CODES.hasOwnProperty(err.code)) {
      err.code = ERROR_CODES[err.code];
    }
    return err;
  }
  
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
        debug('db-migrator:mongo :: connection successful');
        resolve();
      }).catch((error) => {
        debug('db-migrator:mongo :: connection failed', error);
        return reject(error)
      });
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
    return new Promise((resolve, reject) => {
      this.database.collection(to.collection).insertOne(document).then(res => {
        resolve(res);
      }).catch(err => {
        reject(this.getDocumentError(err));
      });
    });
  }

  countDocumentsQuery(collection, query) {
    return new Promise((resolve, reject) => {
      this.database.collection(collection).countDocuments(query).then(res => {
        resolve(res);
        debug('db-migrator:mongo :: count documents query successful');
      }).catch(err => {
        debug('db-migrator:mongo :: count documents query failed', err);
        reject(err);
      })
    });
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
        if (err) {
          debug('db-migrator:mongo :: count documents aggregate failed', err);
          return reject(err);
        }
        debug('db-migrator:mongo :: count documents aggregate successful');
        resolve(res.length ? res[0].count : 0);
      });
    });
  }

  fetchDocumentsQuery(collection, query, skip, limit, cb) {
    return this.database.collection(collection).find(query).limit(limit).skip(skip)
      .toArray((err, documents) => {
        if (err) {
          debug('db-migrator:mongo :: fetch documents query failed', err);
        } else {
          debug('db-migrator:mongo :: fetch documents query successful');
        }
        cb(err, documents)
      });
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
      (err, documents) => {
        if (err) {
          debug('db-migrator:mongo :: fetch documents aggregate failed', err);
        } else {
          debug('db-migrator:mongo :: fetch documents aggregate successful');
        }
        cb(err, documents)
      }
    );

  }


}

module.exports = MongoConnecter;
