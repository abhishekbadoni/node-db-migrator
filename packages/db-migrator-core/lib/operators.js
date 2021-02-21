/* Add operators here as per the need */
/* Inspired from https://docs.mongodb.com/manual/reference/operator/update */

module.exports = {

  $set: (document, property, arg) => {
    document[property] = arg;
  },

  $unset: (document, property, arg) => {
    if (Object.prototype.hasOwnProperty.call(document, property)) {
      delete document[property];
    }
  },

  $rename: (document, property, arg) => {
    if (Object.prototype.hasOwnProperty.call(document, property)) {
      document[arg] = document[property];
      delete document[property];
    }
  },

  // Set if document do not have property
  $default: (document, property, arg) => {
    if (!Object.prototype.hasOwnProperty.call(document, property)) {
      document[property] = arg;
    }
  },

  // Set if null or empty
  $setIfEmpty: (document, property, arg) => {
    if (Object.prototype.hasOwnProperty.call(document, property) && !document[property]) {
      document[property] = arg;
    }
  },

  $unsetIfEmpty: (document, property, arg) => {
    if (Object.prototype.hasOwnProperty.call(document, property) && !document[property]) {
      delete document[property];
    }
  },

  /* Number Specific Operators  */
  $inc: (document, property, arg) => {
    if (typeof document[property] === 'number' && typeof arg === 'number') {
      document[property] += arg;
    }
  },

  $mul: (document, property, arg) => {
    if (typeof document[property] === 'number' && typeof arg === 'number') {
      document[property] = document[property] * arg;
    }
  },

  $min: (document, property, arg) => {
    if (typeof document[property] === 'number' && typeof arg === 'number') {
      document[property] = Math.min(document[property], arg);
    }
  },

  $max: (document, property, arg) => {
    if (typeof document[property] === 'number' && typeof arg === 'number') {
      document[property] = Math.max(document[property], arg);
    }
  },

  /* Array Specific Operators  */
  $addToSet: (document, property, arg) => {
    if (Array.isArray(document[property]) && !document[property].includes(arg)) {
      document[property].push(arg);
    }
  },

  $pop: (document, property, arg) => {
    if (Array.isArray(document[property]) && [-1, 1].includes(arg)) {
      arg === -1 ? document[property].shift() : document[property].pop();
    }
  },

  $pull: (document, property, arg) => {
    if (Array.isArray(document[property]) && document[property].includes(arg)) {
      const index = document[property].indexOf(arg);
      index > -1 && document[property].splice(index, 1);
    }
  },

  $push: (document, property, arg) => {
    if (Array.isArray(document[property])) {
      document[property].push(arg);
    }
  },

  $pullAll: (document, property, arg) => {
    if (Array.isArray(document[property]) && Array.isArray(arg)) {
      document[property] = document[property].filter(value => !arg.includes(value))
    }
  },

};
