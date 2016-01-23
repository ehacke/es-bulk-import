"use strict";
var fs       = require('fs');
var path     = require('path');
var readline = require('readline');

var ProgressBar     = require('progress');
var _               = require('lodash');
var commandLineArgs = require('command-line-args');
var elasticsearch   = require('elasticsearch');
var bunyan          = require('bunyan');
var PrettyStream    = require('bunyan-prettystream');
var prettyStdOut    = new PrettyStream({mode: 'dev'});
prettyStdOut.pipe(process.stdout);

/**
 * Configure general logging
 */
var logger = bunyan.createLogger({
  name:    require('./package').name,
  streams: [
    {
      type:   'raw',
      level:  'debug',
      stream: prettyStdOut
    }
  ]
});

/**
 * Configure ES logging
 */
var esLogger = bunyan.createLogger({
  name:   'es-client',
  type:   'raw',
  level:  'warn',
  stream: prettyStdOut
});

var LogToBunyan = function () {
  var bun      = esLogger;
  this.error   = bun.error.bind(bun);
  this.warning = bun.warn.bind(bun);
  this.info    = bun.info.bind(bun);
  this.debug   = bun.debug.bind(bun);
  this.trace   = function (method, requestUrl, body, responseBody, responseStatus) {
    bun.trace({
      method:         method,
      requestUrl:     requestUrl,
      body:           body,
      responseBody:   responseBody,
      responseStatus: responseStatus
    });
  };
  this.close   = function () { /* bunyan's loggers do not need to be closed */ };
};

/**
 * Bulk import from specified filepath
 *
 * @param filepath
 * @param maxBulk
 * @param totalLines
 */
var processJson = (filepath, maxBulk, totalLines) => {
  const reader = readline.createInterface({
    input: fs.createReadStream(filepath)
  });

  let bar = new ProgressBar('processing [:bar] :percent ETA: :etas', {total: totalLines});

  let body = [];

  reader.on('line', (rawLine) => {
    bar.tick();

    let line = sanitizeLineInput(JSON.parse(rawLine));

    body.push(line);

    if (body.length >= maxBulk) {
      client.bulk({body: body}, function (err) {
        if (err) {
          logger.error('error during bulk:', err.message);
          logger.debug('full error:', err);

          // If connection is lost, stop
          if (err.message === 'No Living connections') {
            throw new Error('No connection to elasticsearch');
          }
        }
      });

      body = [];
    }
  });

  reader.on('close', () => {
    logger.info('Done');
  });
};


/**
 * Sanitize and return an individual line of JSON input
 * @param line
 * @returns {*}
 */
var sanitizeLineInput = (line) => {

  // NOTE:
  // This is just some special sanitation I needed for my case, yours may be different
  if (!_.isUndefined(line.index)) {
    delete line.index._score;
    delete line.index['search.searchTerm.raw'];
    delete line.index['search.refinements.refinement.value.raw'];
    delete line.index['search.refinements.refinement.name.raw'];
  }

  return line;
};

/**
 * Configure CLI options
 */
var cli = commandLineArgs([
  {
    name:  'file',
    alias: 'f',
    type:  String
  },
  {
    name:         'host',
    alias:        'h',
    type:         String,
    defaultValue: 'localhost:9200'
  },
  {
    name:         'max-bulk',
    type:         (input)=> { return (_.isNumber(input) && input > 0); },
    defaultValue: 100
  }
]);

/**
 * Parse CLI options
 * @type {number|{root, dir, base, ext, name}|Object|*}
 */
var options = cli.parse();
if (!_.isString(options.file) || options.file.length === 0) {
  throw new Error('--file option must be provided');
}

/**
 * Configure elasticsearch
 */
var client = new elasticsearch.Client({
  apiVersion: '1.7',
  host:       options.host,
  log:        LogToBunyan
});


/**
 * Determine filepath
 */
var filepath = null;
if (path.isAbsolute(options.file)) {
  filepath = options.file;
} else {
  filepath = path.join(__dirname, options.file);
}

var totalLines = 0;

fs.createReadStream(filepath).on('data', (chunk) => {
  for (var i = 0; i < chunk.length; ++i)
    if (chunk[i] == 10) totalLines++;
}).on('end', () => {
  logger.info('Found ' + totalLines + ' to process');
  return processJson(filepath, options['max-bulk'], totalLines);
});
