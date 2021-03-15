const operators = require('./operators');
const functions = require('./functions');
const connectors = require('./connectors');
const constants = require('./constants');
const errors = require('./errors');
const utils = require('./utils');
const debug = require('debug')('db-migrator:core');
const BaseConnector = require('./helpers/base-connector');

const DEFAULT_OPTIONS = {
  ignoreDuplicates: false
}
class DbMigrator {

  constructor(options) {
    this.initialize(options);
  }

  // Source database name
  get sourceDb() {
    return this.sourceConnector && this.sourceConnector.getDatabaseName();
  }

  // Target database name
  get targetDb() {
    return this.targetConnector && this.targetConnector.getDatabaseName();
  }

  // Connect source database
  connectSourceDatabase(connector, config) {
    return new Promise((resolve, reject) => {
      if (!this.hasConnector(connector)) return reject(errors.invalidConnector(connector));
      debug(`db-migrator:core :: connecting source ${connector}`, config);
      this.sourceConnector = new this.connectors[connector]();
      this.sourceConnector.connectDatabase(config)
        .then(res => resolve(res))
        .catch(err => reject(err))
    })
  }

  // Connect target database
  connectTargetDatabase(connector, config) {
    return new Promise((resolve, reject) => {
      if (!this.hasConnector(connector)) return reject(errors.invalidConnector(connector));
      debug(`db-migrator:core :: connecting target ${connector}`, config);
      this.targetConnector = new this.connectors[connector]();
      this.targetConnector.connectDatabase(config)
        .then(res => resolve(res))
        .catch(err => reject(err))
    });
  }

  // Set in migrators variable
  setMigrators(migrators) {
    debug(`db-migrator:core :: setting migrators`, migrators);
    this.migrators = Array.isArray(migrators) ? migrators : [migrators];
  }

  // validate all the migrators if they are valid or not and set in migrators variable
  validateMigrators(migrators) {
    debug(`db-migrator:core :: validating migrators`);
    const errorsArray = []
    if (utils.isEmpty(migrators)) {
      errorsArray.push(errors.emptyMigrator());
      return errorsArray;
    }
    migrators = Array.isArray(migrators) ? migrators : [migrators];
    migrators.forEach(migrator => {
      const migratorErrors = this.validateMigrator(migrator);
      if (migratorErrors.length) errorsArray.push(...migratorErrors);
    });

    return errorsArray;
  }

  // Validator single migrator - from, to, transform and properties
  validateMigrator({ from = {}, to = {}, transform = null, properties = {} }) {
    const errorsArray = [];
    // Validate from object
    if (utils.isObject(from) && !utils.isEmpty(from)) {
      errorsArray.push(...this.sourceConnector.validateSourceMigrator(from));

      if (from.hasOwnProperty('batch') && !utils.isPositiveInteger(from.batch)) {
        errorsArray.push(errors.invalidBatch());
      }

    } else {
      errorsArray.push(errors.invalidFrom());
    }

    // Validate to object
    if (utils.isObject(to) && !utils.isEmpty(to)) {
      errorsArray.push(...this.targetConnector.validateTargetMigrator(to));
    } else {
      errorsArray.push(errors.invalidTo());
    }

    // Validate transform key
    if (!utils.isEmpty(transform) && !utils.isFunction(transform)) {
      errorsArray.push(errors.invalidTransform());
    }

    // Validate properties
    if (!utils.isObject(properties)) {
      errorsArray.push(errors.invalidProperties());
    } else if (!utils.isEmpty(properties)) {
      errorsArray.push(...this.validateMigratorProperties(properties));
    }
    return errorsArray;
  }

  // Validate properties
  validateMigratorProperties(properties) {
    const errorsArray = [];
    Object.keys(properties).forEach((property) => {

      const operators = properties[property];

      if (!utils.isFunction(operators) && !utils.isObject(operators)) {
        errorsArray.push(errors.invalidProperty(property));
      }

      if (utils.isFunction(operators)) {
        return errorsArray;
      }

      if (utils.isEmpty(operators)) {
        errorsArray.push(errors.invalidProperty(property));
        return errorsArray;
      }

      Object.keys(operators).forEach((operator) => {
        if (!this.hasOperator(operator)) {
          errorsArray.push(errors.invalidOperator(operator));
        }
        const value = operators[operator];
        if (typeof value === 'string' && value.startsWith('fn.')) {
          const fn = value.replace('fn.', '');
          if (!this.hasFunction(fn)) {
            errorsArray.push(errors.invalidFunction(fn))
          }
        }
      });

    });

    return errorsArray;
  }


  // Migrate all the collections one by one in the given order.
  migrate() {
    (async () => {
      const migratorCount = Object.keys(this.migrators).length;
      let migratorIndex = 0;
      this.log(`Database :: ${this.sourceDb} => ${this.targetDb} :: Starting Migration . . . `, false, true);
      for (const key in this.migrators) {
        migratorIndex += 1;
        if (Object.prototype.hasOwnProperty.call(this.migrators, key)) {
          const migrator = this.migrators[key];
          const { from, to } = migrator;
          this.log(`collection :: [ ${migratorIndex} / ${migratorCount} ] :: ${from.collection} => ${to.collection} :: Started`, true, false);
          try {
            const migrated = await this.migrateCollection(key, migrator);
          } catch (error) {
            console.error(error);
            process.exit();
          }
          this.log(`collection :: [ ${migratorIndex} / ${migratorCount} ] :: ${from.collection} => ${to.collection} :: Completed`, true, false);
        }
      }
      this.log(`Database :: ${this.sourceDb} => ${this.targetDb} :: Migration Completed! :)`, false, true);
      process.exit();
    })();
  }

  // Migrate single collection - name is the name of migrator and migrator is migrator configs
  async migrateCollection(name, migrator) {
    this.resetCount();
    let totalDocuments = await this.sourceConnector.countDocuments(migrator.from);
    if (totalDocuments === 0) {
      return true;
    }

    this.setStatistics('totalDocuments', totalDocuments);
    migrator.from.skip = migrator.from.skip || 0;
    migrator.from.batch = migrator.from.batch || constants.FROM_BATCH;
    while (false === await this.migrateCollectionBatch(migrator)) {
      migrator.from.skip += migrator.from.batch;
    }
    return true;
  }

  // Fetch All the documents from source collection of source database
  // and executes the callback multiple times in specific intervals
  migrateCollectionBatch(migrator) {
    return new Promise((resolve, reject) => {
      const { from, to, transform, properties } = migrator;
      this.sourceConnector.fetchDocuments(from, from.skip, from.batch, (err, sourceDocuments) => {
        if (err) return reject(err);
        if (sourceDocuments.length === 0) return resolve(true);

        const fetchedDocuments = this.getStatistics('fetchedDocuments') + sourceDocuments.length;
        this.setStatistics('fetchedDocuments', fetchedDocuments);

        sourceDocuments.forEach((sourceDocument) => {
          const targetDocument = this.transformDocument(sourceDocument, transform, properties);
          this.targetConnector.storeDocument(to, targetDocument)
            .then(() => {
              this.setStatistics('migratedDocuments', this.getStatistics('migratedDocuments') + 1);
              if (this.getStatistics('fetchedDocuments') === this.getStatistics('migratedDocuments')) {
                resolve(false);
              }
            })
            .catch((err) => {
              if ( this.getOption('ignoreDuplicates') &&  err.code === 'DUPLICATE_DOCUMENT') {
                this.setStatistics('ignoredDocuments', this.getStatistics('ignoredDocuments') + 1);
                resolve(false);
              }
              reject(err)
            });
        });
      });
    });
  }

  // Process and transform a document as per the need.
  // this function calls the operators and functions
  transformDocument(document, transform, properties) {

    // If there is a transform function to modify each record
    if (transform && utils.isFunction(transform)) {
      return transform(document);
    }

    for (const property in properties) {
      if (properties.hasOwnProperty(property)) {
        const value = properties[property];

        if (typeof value === 'function') {
          properties[key](document); return;
        }

        for (const operator in value) {
          if (value.hasOwnProperty(operator) && this.hasOperator(operator)) {
            this.executeOperator(operator, document, property, value[operator]);
          }
        }

      }
    }
    return document;
  }

  initialize(options) {
    this.statistics = {};

    this.connectors = {};
    this.operators = {};
    this.functions = {};

    this.addConnectors(connectors);
    this.addOperators(operators);
    this.addFunctions(functions);

    this.setOptions(options);
  }

  setOptions(options) {
    this.options = { ...DEFAULT_OPTIONS, ...options };
  }

  getOption(key) {
    return this.options[key];
  }

  // load sub module - connectors, operators and functions
  loadModule(module) {
    debug(`db-migrator:core :: loading module`, module.name);
    const { name = '', connectors = [], operators = [], functions = [] } = module;
    if (!utils.isEmpty(connectors)) this.addConnectors(connectors);
    if (!utils.isEmpty(operators)) this.addOperators(operators);
    if (!utils.isEmpty(functions)) this.addFunctions(functions);
  }

  hasConnector(connector) {
    return this.connectors.hasOwnProperty(connector);
  }

  getConnector(connector) {
    return this.connectors[connector];
  }

  addConnector(connector, connectorModule) {
    debug(`db-migrator:core :: adding connector`, connector);
    this.connectors[connector] = connectorModule;
  }

  removeConnector(connector) {
    debug(`db-migrator:core :: removing connector`, connector);
    this.connectors[connector] = null;
  }

  addConnectors(connectors) {
    for (const connector in connectors) {
      if (connectors.hasOwnProperty(connector)) {
        this.addConnector(connector, connectors[connector]);
      }
    }
  }

  hasOperator(operator) {
    return this.operators.hasOwnProperty(operator);
  }

  getOperator(operator) {
    return this.operators[operator];
  }

  addOperator(operator, fn) {
    debug(`db-migrator:core :: adding operator`, operator);
    this.operators[operator] = fn;
  }

  removeOperator(operator) {
    debug(`db-migrator:core :: removing operator`, operator);
    this.operators[operator] = null;
  }

  addOperators(operators) {
    for (const operator in operators) {
      if (operators.hasOwnProperty(operator)) {
        this.addOperator(operator, operators[operator]);
      }
    }
  }

  hasFunction(name) {
    return this.functions.hasOwnProperty(name);
  }

  getFunction(name) {
    return this.functions[name];
  }

  addFunction(name, fn) {
    debug(`db-migrator:core :: adding function`, name);
    this.functions[name] = fn;
  }

  removeFunction(name) {
    debug(`db-migrator:core :: removing function`, name);
    this.functions[name] = null;
  }

  addFunctions(functions) {
    for (const name in functions) {
      if (functions.hasOwnProperty(name)) {
        this.addFunction(name, functions[name]);
      }
    }
  }

  // Execute the operator
  executeOperator(operator, document, property, arg) {
    if (Array.isArray(arg)) {
      arg = arg.map(arg => this.executeFunction(arg));
    } else {
      arg = this.executeFunction(arg);
    }
    if (!this.hasOperator(operator)) return this.error(errors.INVALID_OPERATOR)
    this.operators[operator](document, property, arg);
  }

  // execute the function if there is any used from the functions.js file
  executeFunction(string) {
    if (typeof string !== 'string' || !string.startsWith('fn.')) {
      return string;
    }
    const fn = string.replace('fn.', '');
    if (!this.hasFunction(fn)) return string;
    return this.functions[fn]();
  }

  setStatistics(key, value) {
    if (typeof key === 'object' && Object.keys(key).length) {
      Object.keys(key).forEach(subKey => {
        this.setStatistics(subKey, key[subKey]);
      })
      return;
    }
    if (typeof key === 'string') this.statistics[key] = value;
    this.logStatistics('');
  }

  getStatistics(key) {
    if (key) return this.statistics[key];
    return this.statistics;
  }

  // Reset documents count - called at the begining of migrateCollection
  resetCount() {
    this.setStatistics({
      fetchedDocuments: 0,
      migratedDocuments: 0,
      totalDocuments: 0,
      ignoredDocuments: 0,
    });
  }

  // throw error codes as per the error js file - not used as of now.
  error(errorCode) {
    throw new Error(errors[errorCode] || errorCode);
  }

  // Logs important information to the console
  log(str, newline = false, higlight = false) {
    if (higlight) console.log('=================================================================');
    console.log(`${newline ? '\n' : ''}DbMigrator :: ${str}`);
    if (higlight) console.log('=================================================================');
  }

  // Log progress of fetched, migrated and total count of a collection
  logStatistics() {
    const { fetchedDocuments, migratedDocuments, ignoredDocuments, totalDocuments } = this.statistics;
    const string = ` Total: ${totalDocuments}  ::  Fetched: ${fetchedDocuments}  ::  Migrated: ${migratedDocuments}  ::  Ignored: ${ignoredDocuments} `;
    const percent = (migratedDocuments + ignoredDocuments) && Math.round((migratedDocuments + ignoredDocuments) / totalDocuments * 100 * 10) / 10;
    process.stdout.clearLine();
    process.stdout.cursorTo(0);
    process.stdout.write(`DbMigrator :: collection :: ${percent}% :: ${string}`);
  }

}

module.exports = DbMigrator;
