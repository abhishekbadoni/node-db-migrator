const operators = require('./operators');
const functions = require('./functions');
const connectors = require('./connectors');
const constants = require('./constants');
const errors = require('./errors');
const utils = require('./utils');

class DbMigrator {

  constructor() {
    this.initialize();
  }

  get sourceDb() {
    return this.sourceConnector && this.sourceConnector.getDatabaseName();
  }

  get targetDb() {
    return this.targetConnector && this.targetConnector.getDatabaseName();
  }

  connectSourceDatabase(connector, config) {
    return new Promise((resolve, reject) => {
      if (!this.hasConnector(connector)) return reject(errors.invalidConnector(connector));
      this.sourceConnector = new this.connectors[connector]();
      this.sourceConnector.connectDatabase(config)
        .then(res => resolve(res))
        .catch(err => reject(err))
    })
  }

  connectTargetDatabase(connector, config) {
    return new Promise((resolve, reject) => {
      if (!this.hasConnector(connector)) return reject(errors.invalidConnector(connector));
      this.targetConnector = new this.connectors[connector]();
      this.targetConnector.connectDatabase(config)
        .then(res => resolve(res))
        .catch(err => reject(err))
    });
  }

  // validate all the migrators if they are valid or not and set in migrators variable
  setMigrators(migrators) {
    this.migrators = Array.isArray(migrators) ? migrators : [migrators];
  }

  validateMigrators(migrators) {
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

  validateMigrator({ from = {}, to = {}, properties = {} }) {
    const errorsArray = [];
    if (utils.isObject(from) && !utils.isEmpty(from)) {
      errorsArray.push(...this.sourceConnector.validateSourceMigrator(from));

      if (from.hasOwnProperty('batch') && !utils.isPositiveInteger(from.batch)) {
        errorsArray.push(errors.invalidBatch());
      }

    } else {
      errorsArray.push(errors.invalidFrom());
    }

    if (utils.isObject(to) && !utils.isEmpty(to)) {
      errorsArray.push(...this.targetConnector.validateTargetMigrator(to));
    } else {
      errorsArray.push(errors.invalidTo());
    }

    if (!utils.isObject(properties)) {
      errorsArray.push(errors.invalidProperties());
    } else if (!utils.isEmpty(properties)) {
      errorsArray.push(...this.validateMigratorProperties(properties));
    }
    return errorsArray;
  }

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
      const { from, to, properties } = migrator;
      this.sourceConnector.fetchDocuments(from, from.skip, from.batch, (err, sourceDocuments) => {
        if (err) return reject(err);
        if (sourceDocuments.length === 0) return resolve(true);

        const fetchedDocuments = this.getStatistics('fetchedDocuments') + sourceDocuments.length;
        this.setStatistics('fetchedDocuments', fetchedDocuments);

        sourceDocuments.forEach((sourceDocument) => {
          const targetDocument = this.transformDocument(sourceDocument, properties);
          this.targetConnector.storeDocument(to, targetDocument)
            .then(() => {
              this.setStatistics('migratedDocuments', this.getStatistics('migratedDocuments') + 1);
              if (this.getStatistics('fetchedDocuments') === this.getStatistics('migratedDocuments')) {
                resolve(false);
              }
            })
            .catch((err) => reject(err));
        });
      });
    });
  }

  // Process and transform a document as per the need.
  // this function calls the operators and functions
  transformDocument(document, properties) {
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

  initialize() {
    this.statistics = {};

    this.connectors = {};
    this.operators = {};
    this.functions = {};

    this.addConnectors(connectors);
    this.addOperators(operators);
    this.addFunctions(functions);
  }

  loadModule(module) {
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
    console.log('addConnector');
    console.log(connector);
    this.connectors[connector] = connectorModule;
  }

  removeConnector(connector) {
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
    this.operators[operator] = fn;
  }

  removeOperator(operator) {
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
    this.functions[name] = fn;
  }

  removeFunction(name) {
    this.functions[name] = null;
  }

  addFunctions(functions) {
    for (const name in functions) {
      if (functions.hasOwnProperty(name)) {
        this.addFunction(name, functions[name]);
      }
    }
  }

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
    const { fetchedDocuments, migratedDocuments, totalDocuments } = this.statistics;
    const string = `Fetched ${fetchedDocuments} and Migrated ${migratedDocuments} of ${totalDocuments}`;
    process.stdout.clearLine();
    process.stdout.cursorTo(0);
    process.stdout.write(`DbMigrator :: collection :: ${string}`);
  }

}

module.exports = DbMigrator;
