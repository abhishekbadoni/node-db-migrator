exports.isEmpty = (value) => {
    if (value === null) return true;
    if (value === undefined) return true;
    if (Array.isArray(value)) return value.length === 0;
    if (typeof value === 'object') return Object.keys(value).length === 0;
    return !value;
}

exports.isPositiveInteger = (value) => {
    return typeof value === 'number' && value > 0;
}

exports.isObject = (value) => {
    return typeof value === 'object';
}

exports.isFunction = (value) => {
    return typeof value === 'function';
}