# `db-migrator-mongo`

> A mongo sub-package for database migrator **db-migrator-core**. 

## Usage

```js
const { DbMigrator } = require('db-migrator-core');
const DbMigratorMongo = require('db-migrator-mongo');

const migrator = new DbMigrator();
migrator.loadModule(DbMigratorMongo); // load any sub-package

// migration configs
const migrators = [
    {
        from: { collection: 'students' },
        to: { collection: 'students_copy' },
    },
    {
        from: { collection: 'techers' },
        to: { collection: 'techers_copy' },
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

        console.log('Migration Succeeded !')

    } catch(err) {
        console.error(err);
    }

})();
```
