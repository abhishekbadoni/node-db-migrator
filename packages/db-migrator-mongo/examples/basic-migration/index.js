const { DbMigrator } = require('../../../db-migrator-core');
const DbMigratorMongo = require('../../')

const sourceConfig = {
  url: 'mongodb://localhost:27017/db_01',
  database: 'db_02',
}
const targetConfig = {
  url: 'mongodb://localhost:27017/db_01',
  database: 'db_02',
}

const migrators = [
  {
    from: { collection: 'students' },
    to: { collection: 'students_copy' },
    properties: {
      _id: {
        $set: 'fn.uuid.v4',
      }
    },
  },
];

(async () => {

  try {

    // Prepare migrators
    const migrator = new DbMigrator();
    migrator.loadModule(DbMigratorMongo);

    // Connect source and target databases
    await migrator.connectSourceDatabase('mongo', sourceConfig);
    await migrator.connectTargetDatabase('mongo', targetConfig);

    // Validate migrators
    const errors = migrator.validateMigrators(migrators);
    if (errors.length) return console.error(errors);

    // Do Migration
    migrator.setMigrators(migrators);
    await migrator.migrate();        

  } catch (err) {
    console.error(err);
  }

})();




