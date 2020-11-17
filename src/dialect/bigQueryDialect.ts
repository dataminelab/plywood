import { Duration, Timezone } from 'chronoshift';
import { PlyType, PlyTypeSimple } from '../types';
import { SQLDialect } from './baseDialect';

export class BigQueryDialect extends SQLDialect {
  static TIME_BUCKETING: Record<string, string> = {
    PT1S: '%Y-%m-%d %H:%i:%SZ',
    PT1M: '%Y-%m-%d %H:%i:00Z',
    PT1H: '%Y-%m-%d %H:00:00Z',
    P1D: '%Y-%m-%d 00:00:00Z',
    P1M: '%Y-%m-01 00:00:00Z',
    P1Y: '%Y-01-01 00:00:00Z'
  };

  static CAST_TO_FUNCTION: Record<string, Record<string, string>> = {
    TIME: {
      NUMBER: 'TIMESTAMP_MILLIS($$)',
    },
    NUMBER: {
      TIME: 'UNIX_MILLIS($$)',
      STRING: 'cast($$ as NUMERIC)',
    },
    STRING: {
      NUMBER: 'cast($$ as string)',
    },
  };

  // TODO Complete the rest of functions
  static TIME_PART_TO_FUNCTION: Record<string, string> = {
    SECOND_OF_MINUTE: "extract(SECOND from $$)",
    // SECOND_OF_HOUR: "(DATE_PART('minute',$$)*60+DATE_PART('second',$$))",
    // SECOND_OF_DAY: "((DATE_PART('hour',$$)*60+DATE_PART('minute',$$))*60+DATE_PART('second',$$))",
    // SECOND_OF_WEEK:
    //   "((((CAST((DATE_PART('dow',$$)+6) AS int)%7)*24)+DATE_PART('hour',$$)*60+DATE_PART('minute',$$))*60+DATE_PART('second',$$))",
    // SECOND_OF_MONTH:
    //   "((((DATE_PART('day',$$)-1)*24)+DATE_PART('hour',$$)*60+DATE_PART('minute',$$))*60+DATE_PART('second',$$))",
    // SECOND_OF_YEAR:
    //   "((((DATE_PART('doy',$$)-1)*24)+DATE_PART('hour',$$)*60+DATE_PART('minute',$$))*60+DATE_PART('second',$$))",
    //
    MINUTE_OF_HOUR: "extract(MINUTE from $$)",
    // MINUTE_OF_DAY: "DATE_PART('hour',$$)*60+DATE_PART('minute',$$)",
    // MINUTE_OF_WEEK:
    //   "((CAST((DATE_PART('dow',$$)+6) AS int)%7)*24)+DATE_PART('hour',$$)*60+DATE_PART('minute',$$)",
    // MINUTE_OF_MONTH: "((DATE_PART('day',$$)-1)*24)+DATE_PART('hour',$$)*60+DATE_PART('minute',$$)",
    // MINUTE_OF_YEAR: "((DATE_PART('doy',$$)-1)*24)+DATE_PART('hour',$$)*60+DATE_PART('minute',$$)",
    //
    HOUR_OF_DAY: "EXTRACT(HOUR from $$)",
    // HOUR_OF_WEEK: "((CAST((DATE_PART('dow',$$)+6) AS int)%7)*24+DATE_PART('hour',$$))",
    // HOUR_OF_MONTH: "((DATE_PART('day',$$)-1)*24+DATE_PART('hour',$$))",
    // HOUR_OF_YEAR: "((DATE_PART('doy',$$)-1)*24+DATE_PART('hour',$$))",
    //
    DAY_OF_WEEK: "extract(DAYOFWEEK from $$)",
    DAY_OF_MONTH: "extract(DAY from $$)",
    DAY_OF_YEAR: "extract(DAYOFYEAR from $$)",
    //
    WEEK_OF_YEAR: "EXTRACT(week from $$)",
    //
    MONTH_OF_YEAR: "EXTRACT(month from $$)",
    YEAR: "EXTRACT(year from $$)",
  };

  public escapeName(name: string): string {
    name = name.replace(/`/g, '``');
    return '`' + name + '`';
  }

  public castExpression(inputType: PlyType, operand: string, cast: string): string {
    let castFunction = BigQueryDialect.CAST_TO_FUNCTION[cast][inputType];
    if (!castFunction)
      throw new Error(`unsupported cast from ${inputType} to ${cast} in MySQL dialect`);
    return castFunction.replace(/\$\$/g, operand);
  }

  public extractExpression(operand: string, regexp: string): string {
    // https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_extract
    return `REGEXP_EXTRACT(${operand}, '${regexp}'))`;
  }

  public indexOfExpression(str: string, substr: string): string {
    // https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#strpos
    return `STRPOS(${substr} IN ${str}) - 1`;
  }

  public timeBucketExpression(operand: string, duration: Duration, timezone: Timezone): string {
    return this.timeFloorExpression(operand, duration, timezone);
  }

  timeFloorExpression(operand: string, duration: Duration, timezone: Timezone): string {
    let bucketFormat = BigQueryDialect.TIME_BUCKETING[duration.toString()];
    if (!bucketFormat) throw new Error(`unsupported duration '${duration}'`);
    return this.walltimeToUTC(
      `DATE_FORMAT(${this.utcToWalltime(operand, timezone)},'${bucketFormat}')`,
      timezone,
    );
  }

  public timePartExpression(operand: string, part: string, timezone: Timezone): string {
    // https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#extract
    let timePartFunction = BigQueryDialect.TIME_PART_TO_FUNCTION[part];
    if (!timePartFunction) throw new Error(`unsupported part ${part} in Postgres dialect`);
    return timePartFunction.replace(/\$\$/g, this.utcToWalltime(operand, timezone));
  }

  public regexpExpression(expression: string, regexp: string): string {
    // https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#regexp_contains
    return `REGEXP_CONTAINS(${expression}, ${this.escapeLiteral(regexp)})`;
  }

  public containsExpression(a: string, b: string): string {
    return `STRPOS(${a},${b})>0`;
  }


  concatExpression(a: string, b: string): string {
    return `CONCAT(${a},${b})`;
  }

  timeShiftExpression(operand: string, duration: Duration, step: int, timezone: Timezone): string {
    if (step === 0) return operand;

    // https://cloud.google.com/bigquery/docs/reference/standard-sql/datetime_functions#datetime_add
    let sqlFn = step > 0 ? 'DATETIME_ADD(' : 'DATETIME_SUB(';
    let spans = duration.multiply(Math.abs(step)).valueOf();
    if (spans.week) {
      operand = sqlFn + operand + ', INTERVAL ' + String(spans.week) + ' WEEK)';
    }
    if (spans.month) {
      let expr = String(spans.month);
      operand = sqlFn + operand + ", INTERVAL " + expr + " MONTH)";
    }
    if (spans.year) {
      let expr = String(spans.year);
      operand = sqlFn + operand + ", INTERVAL " + expr + " YEAR)";
    }
    if (spans.day) {
      let expr = String(spans.day);
      operand = sqlFn + operand + ", INTERVAL " + expr + " DAY)";
    }
    if (spans.hour) {
      let expr = String(spans.hour);
      operand = sqlFn + operand + ", INTERVAL " + expr + " HOUR)";
    }
    if (spans.minute) {
      let expr = String(spans.minute);
      operand = sqlFn + operand + ", INTERVAL " + expr + " MINUTE)";
    }
    if (spans.second) {
      let expr = spans.second;
      operand = sqlFn + operand + ", INTERVAL '" + expr + "' SECOND)";
    }
    return operand;
  }

  timeToSQL(date: Date): string {
    // format: '2020-11-17 15:11:12.086418 UTC'
    if (!date) return this.nullConstant();
    return `TIMESTAMP('${date.toISOString()}')`;
  }

  public utcToWalltime(operand: string, timezone: Timezone): string {
    // todo figure out timezone witchcraft
    return operand;
  }

  public walltimeToUTC(operand: string, timezone: Timezone): string {
    // todo figure out timezone witchcraft
    return operand;
  }
}
