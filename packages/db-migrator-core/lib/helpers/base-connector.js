class BaseConnector {

  constructor() {
  }
  
  getDatabaseName() {
    return this.config.database;
  }

}

module.exports = BaseConnector;
