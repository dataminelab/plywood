import { Duration, Timezone } from 'chronoshift';
import { PlyType, PlyTypeSimple } from '../types';
import { SQLDialect } from './baseDialect';

export class BigQueryDialect extends SQLDialect {
  castExpression(inputType: PlyType, operand: string, cast: PlyTypeSimple): string {
    return '';
  }

  extractExpression(operand: string, regexp: string): string {
    return '';
  }

  indexOfExpression(str: string, substr: string): string {
    return '';
  }

  timeBucketExpression(operand: string, duration: Duration, timezone: Timezone): string {
    return '';
  }

  timeFloorExpression(operand: string, duration: Duration, timezone: Timezone): string {
    return '';
  }

  timePartExpression(operand: string, part: string, timezone: Timezone): string {
    return '';
  }

  timeShiftExpression(operand: string, duration: Duration, step: int, timezone: Timezone): string {
    return '';
  }

  timeToSQL(date: Date): string {
    return '';
  }
}
