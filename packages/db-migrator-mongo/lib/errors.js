exports.invalidFromCollection = () => ({
    message: 'from.collection should be a valid collection name',
    code: 'INVALID_FROM_COLLECTION'
});

exports.invalidFromQuery = () => ({
    message: 'form.query should be a valid object',
    code: 'INVALID_FROM_QUERY'
});

exports.invalidFromAggregate = () => ({
    message: 'form.aggregate should be a valid array',
    code: 'INVALID_FROM_AGGREGATE'
});

exports.invalidToCollection = () => ({
    message: 'to.collection should be a valid collection name',
    code: 'INVALID_TO_COLLECTION'
});
