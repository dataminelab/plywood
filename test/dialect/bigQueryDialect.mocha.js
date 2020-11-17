const { expect } = require('chai');
const { PassThrough } = require('readable-stream');
let { sane } = require('../utils');

let plywood = require('../plywood');
let { BigQueryDialect } = plywood;

let dialect = new BigQueryDialect()

describe('BigQueryDialect', () => {
  describe('should format date to iso', () => {
    expect(dialect.timeToSQL(new Date("2015-09-12T23:59:00.000Z"))).to.be.equal('TIMESTAMP(\'2015-09-12T23:59:00.000Z\')')
  })
});