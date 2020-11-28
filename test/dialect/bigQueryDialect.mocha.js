const { expect } = require('chai');
const { Duration, Timezone }  = require('chronoshift');

let plywood = require('../plywood');
let { BigQueryDialect } = plywood;

let dialect = new BigQueryDialect()

describe('BigQueryDialect', () => {
  describe('should format date to iso', () => {
    expect(dialect.timeToSQL(new Date("2015-09-12T23:59:00.000Z"))).to.be.equal('TIMESTAMP(\'2015-09-12T23:59:00.000Z\')')
  })

  describe('should generate time shift expressions', () => {
    let str = dialect.timeShiftExpression('CURRENT_DATETIME', Duration.fromJS('P1W'), 1, new Timezone('UTC'))
    expect(str).to.be.equal('DATETIME_ADD(CURRENT_DATETIME, INTERVAL 1 WEEK)')

    const oneOfEach = Duration.fromJS('P1Y1M1DT1H1M1S');
    str = dialect.timeShiftExpression('CURRENT_DATETIME', oneOfEach, 1, new Timezone('UTC'))

    expect(str).to.be.equal(
      'DATETIME_ADD(' +
        'DATETIME_ADD(' +
          'DATETIME_ADD(' +
            'DATETIME_ADD(' +
              'DATETIME_ADD(' +
                'DATETIME_ADD(CURRENT_DATETIME, ' +
                'INTERVAL 1 MONTH), ' +
              'INTERVAL 1 YEAR), ' +
            'INTERVAL 1 DAY), ' +
          'INTERVAL 1 HOUR), ' +
        'INTERVAL 1 MINUTE), ' +
      'INTERVAL 1 SECOND)')
  })
});