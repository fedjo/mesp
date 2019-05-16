/* handle warning and error messages */

const path = require('path');

const warnings = {};
const errors = {};
const validSchemas = {};
const validExamples = {};
const supportedExamples = {};

const addMessageToMap = function(modelPath, message, map) {
  const rootModel = getRootModelName(modelPath);
  const fullMessage = modelPath + ': ' + message;
  if (map[rootModel] != null) {
    map[rootModel].push(fullMessage);
  } else {
    map[rootModel] = [fullMessage];
  }
  return true;
};

//given a path, retrieve the name of the root model
const getRootModelName = function(fullPath) {
  const index = fullPath.indexOf(path.sep);
  if (index > 0) {
    return fullPath.substring(0, index);
  }
  return fullPath;
};

module.exports = {
  warnings,
  errors,
  validSchemas,
  validExamples,
  supportedExamples,

  //add warning to the warnings map for a given model
  addWarning(modelPath, message) {
    return addMessageToMap(modelPath, message, warnings);
  },

  //add error to the errors map for a given model
  addError(modelPath, message) {
    return addMessageToMap(modelPath, message, errors);
  },

  //add valid schema to the valid schemas map for a given model
  addValidSchema(modelPath, message) {
    return addMessageToMap(modelPath, message, validSchemas);
  },

  //add valid example to the valid examples map for a given model
  addValidExample(modelPath, message) {
    return addMessageToMap(modelPath, message, validExamples);
  },
  //add valid example to the valid examples map for a given model
  addSupportedExample(modelPath, message) {
    return addMessageToMap(modelPath, message, supportedExamples);
  },
};
