/* Add functions here as per the need */
const uuid = require('uuid');
const { ObjectId } = require('mongodb');

module.exports = {
  'uuid.v5': uuid.v5,
  'uuid.v4': uuid.v4,
  'uuid.v3': uuid.v3,
  'uuid.v1': uuid.v1,
  'objectId': ObjectId,
  'date': () => new Date(),
};