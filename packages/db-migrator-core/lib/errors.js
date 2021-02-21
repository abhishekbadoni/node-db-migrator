exports.invalidConnector = (connector) => ({
    message: `Invalid Connector - ${connector}`,
    code: 'EMPTY_MIGRATOR'
});

exports.emptyMigrator = () => ({
    message: 'Migrator should be a non empty object',
    code: 'EMPTY_MIGRATOR'
});

exports.invalidFrom = () => ({
    message: `from should be a non empty object`,
    code: 'INVALID_FROM',
});

exports.invalidBatch = () => ({
    message: `from.batch should be a positive integer`,
    code: 'INVALID_FROM_BATCH',
});

exports.invalidTo = () => ({
    message: `to should be a non empty object`,
    code: 'INVALID_TO',
});

exports.invalidProperties = () => ({
    message: `properties should be a non empty object`,
    code: 'INVALID_PROPERTIES',
});

exports.invalidOperator = (operator) => ({
    message: `Invalid operator - ${operator}`,
    code: 'INVALID_OPERATOR',
});

exports.invalidProperty = (property) => ({
    message: `Invalid property - ${property}`,
    code: 'INVALID_PROPERTY',
});

exports.invalidFunction = (fn) => ({
    message: `Invalid function - ${fn}`,
    code: 'INVALID_FUNCTION',
});
