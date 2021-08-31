import { PlywoodRequester } from 'plywood-base-api';
import { Attributes } from '../datatypes';
import { AwsAthenaDialect } from '../dialect/awsAthenaDialect';
import { External, ExternalJS, ExternalValue } from './baseExternal';
import { SQLExternal } from './sqlExternal';

export class AwsAthenaExternal extends SQLExternal {
  static engine = 'athena';
  static type = 'DATASET';


  static fromJS(parameters: ExternalJS, requester: PlywoodRequester<any>): AwsAthenaExternal {
    let value: ExternalValue = External.jsToValue(parameters, requester);
    return new AwsAthenaExternal(value);
  }

  constructor(parameters: ExternalValue) {
    super(parameters, new AwsAthenaDialect());
    this._ensureEngine('athena');
  }

  protected getIntrospectAttributes(): Promise<Attributes> {
    throw 'Introspection is done in redash';
  }
}

External.register(AwsAthenaExternal);
