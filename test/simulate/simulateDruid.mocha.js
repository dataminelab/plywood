var { expect } = require("chai");

var { WallTime } = require('chronoshift');
if (!WallTime.rules) {
  var tzData = require("chronoshift/lib/walltime/walltime-data.js");
  WallTime.init(tzData.rules, tzData.zones);
}

var plywood = require('../../build/plywood');
var { Expression, External, Dataset, TimeRange, $, ply, r } = plywood;

var attributes = [
  { name: 'time', type: 'TIME' },
  { name: 'color', type: 'STRING' },
  { name: 'cut', type: 'STRING' },
  { name: 'isNice', type: 'BOOLEAN' },
  { name: 'tags', type: 'SET/STRING' },
  { name: 'carat', type: 'NUMBER' },
  { name: 'height_bucket', special: 'range', separator: ';', rangeSize: 0.05, digitsAfterDecimal: 2 },
  { name: 'price', type: 'NUMBER', unsplitable: true },
  { name: 'tax', type: 'NUMBER', unsplitable: true },
  { name: 'vendor_id', special: 'unique', unsplitable: true }
];

var context = {
  'diamonds': External.fromJS({
    engine: 'druid',
    dataSource: 'diamonds',
    timeAttribute: 'time',
    attributes,
    allowSelectQueries: true,
    filter: $("time").in({
      start: new Date('2015-03-12T00:00:00'),
      end: new Date('2015-03-19T00:00:00')
    }),
    druidVersion: '0.9.1'
  }),
  'diamonds-alt:;<>': External.fromJS({
    engine: 'druid',
    dataSource: 'diamonds-alt:;<>',
    timeAttribute: 'time',
    attributes,
    allowSelectQueries: true,
    filter: $("time").in({
      start: new Date('2015-03-12T00:00:00'),
      end: new Date('2015-03-19T00:00:00')
    }),
    druidVersion: '0.9.1'
  })
};

var contextUnfiltered = {
  'diamonds': External.fromJS({
    engine: 'druid',
    dataSource: 'diamonds',
    timeAttribute: 'time',
    attributes,
    allowSelectQueries: true
  })
};

describe("simulate Druid", () => {
  it("works in basic case", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter($("color").is('D')))
      .apply('Count', '$diamonds.count()')
      .apply('TotalPrice', '$diamonds.sum($price)');

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          },
          {
            "fieldName": "price",
            "name": "TotalPrice",
            "type": "doubleSum"
          }
        ],
        "dataSource": "diamonds",
        "filter": {
          "dimension": "color",
          "type": "selector",
          "value": "D"
        },
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "queryType": "timeseries"
      }
    ]);
  });

  it("works on initial dataset", () => {
    var dataset = Dataset.fromJS([
      { col: 'D' },
      { col: 'E' }
    ]);

    var ex = ply(dataset)
      .apply("diamonds", $('diamonds').filter($("color").is('$col')))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "__VALUE__",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "filter": {
          "dimension": "color",
          "type": "selector",
          "value": "D"
        },
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "queryType": "timeseries"
      },
      {
        "aggregations": [
          {
            "name": "__VALUE__",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "filter": {
          "dimension": "color",
          "type": "selector",
          "value": "E"
        },
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "queryType": "timeseries"
      }
    ]);
  });

  it("works in advanced case", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter($("color").is('D').and($('tags').overlap(['Good', 'Bad', 'Ugly']))))
      .apply('Count', '$diamonds.count()')
      .apply('TotalPrice', '$diamonds.sum($price)')
      .apply('PriceTimes2', '$diamonds.sum($price) * 2')
      .apply('PriceMinusTax', '$diamonds.sum($price) - $diamonds.sum($tax)')
      .apply('PriceDiff', '$diamonds.sum($price - $tax)')
      .apply('Crazy', '$diamonds.sum($price) - $diamonds.sum($tax) + 10 - $diamonds.sum($carat)')
      .apply('PriceAndTax', '$diamonds.sum($price) * $diamonds.sum($tax)')
      .apply('SixtySix', 66)
      .apply('PriceGoodCut', $('diamonds').filter($('cut').is('good')).sum('$price'))
      .apply('AvgPrice', '$diamonds.average($price)')
      .apply(
        'Cuts',
        $("diamonds").split("$cut", 'Cut')
          .apply('Count', $('diamonds').count())
          .sort('$Count', 'descending')
          .limit(2)
          .apply(
            'Time',
            $("diamonds").split($("time").timeBucket('P1D', 'America/Los_Angeles'), 'Timestamp')
              .apply('TotalPrice', $('diamonds').sum('$price'))
              .sort('$Timestamp', 'ascending')
              //.limit(10)
              .apply(
                'Carats',
                $("diamonds").split($("carat").numberBucket(0.25), 'Carat')
                  .apply('Count', $('diamonds').count())
                  .sort('$Count', 'descending')
                  .limit(3)
              )
          )
      );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          },
          {
            "fieldName": "price",
            "name": "TotalPrice",
            "type": "doubleSum"
          },
          {
            "aggregator": {
              "fieldName": "price",
              "name": "PriceGoodCut",
              "type": "doubleSum"
            },
            "filter": {
              "dimension": "cut",
              "type": "selector",
              "value": "good"
            },
            "name": "PriceGoodCut",
            "type": "filtered"
          },
          {
            "fieldName": "tax",
            "name": "!T_0",
            "type": "doubleSum"
          },
          {
            "fieldName": "carat",
            "name": "!T_1",
            "type": "doubleSum"
          }
        ],
        "dataSource": "diamonds",
        "filter": {
          "fields": [
            {
              "dimension": "color",
              "type": "selector",
              "value": "D"
            },
            {
              "fields": [
                {
                  "dimension": "tags",
                  "type": "selector",
                  "value": "Good"
                },
                {
                  "dimension": "tags",
                  "type": "selector",
                  "value": "Bad"
                },
                {
                  "dimension": "tags",
                  "type": "selector",
                  "value": "Ugly"
                }
              ],
              "type": "or"
            }
          ],
          "type": "and"
        },
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "postAggregations": [
          {
            "fields": [
              {
                "fieldName": "TotalPrice",
                "type": "fieldAccess"
              },
              {
                "fieldName": "!T_0",
                "type": "fieldAccess"
              }
            ],
            "fn": "-",
            "name": "PriceDiff",
            "type": "arithmetic"
          },
          {
            "fields": [
              {
                "fieldName": "TotalPrice",
                "type": "fieldAccess"
              },
              {
                "fieldName": "Count",
                "type": "fieldAccess"
              }
            ],
            "fn": "/",
            "name": "AvgPrice",
            "type": "arithmetic"
          },
          {
            "fields": [
              {
                "fieldName": "TotalPrice",
                "type": "fieldAccess"
              },
              {
                "type": "constant",
                "value": 2
              }
            ],
            "fn": "*",
            "name": "PriceTimes2",
            "type": "arithmetic"
          },
          {
            "fields": [
              {
                "fieldName": "TotalPrice",
                "type": "fieldAccess"
              },
              {
                "fieldName": "!T_0",
                "type": "fieldAccess"
              }
            ],
            "fn": "-",
            "name": "PriceMinusTax",
            "type": "arithmetic"
          },
          {
            "fields": [
              {
                "fields": [
                  {
                    "fields": [
                      {
                        "fieldName": "TotalPrice",
                        "type": "fieldAccess"
                      },
                      {
                        "fieldName": "!T_0",
                        "type": "fieldAccess"
                      }
                    ],
                    "fn": "-",
                    "type": "arithmetic"
                  },
                  {
                    "type": "constant",
                    "value": 10
                  }
                ],
                "fn": "+",
                "type": "arithmetic"
              },
              {
                "fieldName": "!T_1",
                "type": "fieldAccess"
              }
            ],
            "fn": "-",
            "name": "Crazy",
            "type": "arithmetic"
          },
          {
            "fields": [
              {
                "fieldName": "TotalPrice",
                "type": "fieldAccess"
              },
              {
                "fieldName": "!T_0",
                "type": "fieldAccess"
              }
            ],
            "fn": "*",
            "name": "PriceAndTax",
            "type": "arithmetic"
          },
          {
            "name": "SixtySix",
            "type": "constant",
            "value": 66
          }
        ],
        "queryType": "timeseries"
      },
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "cut",
          "outputName": "Cut",
          "type": "default"
        },
        "filter": {
          "fields": [
            {
              "dimension": "color",
              "type": "selector",
              "value": "D"
            },
            {
              "fields": [
                {
                  "dimension": "tags",
                  "type": "selector",
                  "value": "Good"
                },
                {
                  "dimension": "tags",
                  "type": "selector",
                  "value": "Bad"
                },
                {
                  "dimension": "tags",
                  "type": "selector",
                  "value": "Ugly"
                }
              ],
              "type": "or"
            }
          ],
          "type": "and"
        },
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "metric": "Count",
        "queryType": "topN",
        "threshold": 2
      },
      {
        "aggregations": [
          {
            "fieldName": "price",
            "name": "TotalPrice",
            "type": "doubleSum"
          }
        ],
        "dataSource": "diamonds",
        "filter": {
          "fields": [
            {
              "dimension": "color",
              "type": "selector",
              "value": "D"
            },
            {
              "fields": [
                {
                  "dimension": "tags",
                  "type": "selector",
                  "value": "Good"
                },
                {
                  "dimension": "tags",
                  "type": "selector",
                  "value": "Bad"
                },
                {
                  "dimension": "tags",
                  "type": "selector",
                  "value": "Ugly"
                }
              ],
              "type": "or"
            },
            {
              "dimension": "cut",
              "type": "selector",
              "value": "some_cut"
            }
          ],
          "type": "and"
        },
        "granularity": {
          "period": "P1D",
          "timeZone": "America/Los_Angeles",
          "type": "period"
        },
        "intervals": "2015-03-12/2015-03-19",
        "queryType": "timeseries",
        "context": {
          "skipEmptyBuckets": "true"
        }
      },
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "carat",
          "extractionFn": {
            "function": "function(d){d=Number(d); if(isNaN(d)) return 'null'; return Math.floor(d / 0.25) * 0.25;}",
            "type": "javascript"
          },
          "outputName": "Carat",
          "type": "extraction"
        },
        "filter": {
          "fields": [
            {
              "dimension": "color",
              "type": "selector",
              "value": "D"
            },
            {
              "fields": [
                {
                  "dimension": "tags",
                  "type": "selector",
                  "value": "Good"
                },
                {
                  "dimension": "tags",
                  "type": "selector",
                  "value": "Bad"
                },
                {
                  "dimension": "tags",
                  "type": "selector",
                  "value": "Ugly"
                }
              ],
              "type": "or"
            },
            {
              "dimension": "cut",
              "type": "selector",
              "value": "some_cut"
            }
          ],
          "type": "and"
        },
        "granularity": "all",
        "intervals": "2015-03-13T07/2015-03-14T07",
        "metric": "Count",
        "queryType": "topN",
        "threshold": 3
      }
    ]);
  });

  it("works on OVERLAP (single value) filter", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.overlap(['D'])"))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "type": "selector",
      "value": "D"
    });
  });

  it("works on OVERLAP (multi value) filter", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.overlap(['C', 'D'])"))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "fields": [
        {
          "dimension": "color",
          "type": "selector",
          "value": "C"
        },
        {
          "dimension": "color",
          "type": "selector",
          "value": "D"
        }
      ],
      "type": "or"
    });
  });

  it("works on fancy filter dataset (EXTRACT / IS)", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.extract('^(.)') == 'D'"))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "extractionFn": {
        "expr": "^(.)",
        "replaceMissingValue": true,
        "type": "regex"
      },
      "type": "extraction",
      "value": "D"
    });
  });

  it("works on fancy filter dataset (EXTRACT + FALLBACK / IS)", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.extract('^(.)').fallback('lol') == 'D'"))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "extractionFn": {
        "expr": "^(.)",
        "replaceMissingValue": true,
        "replaceMissingValueWith": "lol",
        "type": "regex"
      },
      "type": "extraction",
      "value": "D"
    });
  });

  it("works on fancy filter (SUBSTR / IS)", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.substr(0, 1) == 'D'"))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "extractionFn": {
        "type": "substring",
        "index": 0,
        "length": 1
      },
      "type": "extraction",
      "value": "D"
    });
  });

  it("works on fancy filter (SUBSTR / IN)", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.substr(0, 1).in(['D', 'C'])"))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "type": "or",
      "fields": [
        {
          "dimension": "color",
          "extractionFn": {
            "type": "substring",
            "index": 0,
            "length": 1
          },
          "type": "extraction",
          "value": "D"
        },
        {
          "dimension": "color",
          "extractionFn": {
            "type": "substring",
            "index": 0,
            "length": 1
          },
          "type": "extraction",
          "value": "C"
        }
      ]
    });
  });

  it("works on fancy filter (LOOKUP / IN)", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter("$color.lookup('some_lookup').in(['D', 'C'])"))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "fields": [
        {
          "dimension": "color",
          "extractionFn": {
            "lookup": {
              "namespace": 'some_lookup',
              "type": "namespace"
            },
            "type": "lookup"
          },
          "type": "extraction",
          "value": "D"
        },
        {
          "dimension": "color",
          "extractionFn": {
            "lookup": {
              "namespace": 'some_lookup',
              "type": "namespace"
            },
            "type": "lookup"
          },
          "type": "extraction",
          "value": "C"
        }
      ],
      "type": "or"
    });
  });

  it("works on fancy filter [.in(...).not()]", () => {
    var ex = $('diamonds').filter("$color.in(['D', 'C']).not()");

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "field": {
        "fields": [
          {
            "dimension": "color",
            "type": "selector",
            "value": "D"
          },
          {
            "dimension": "color",
            "type": "selector",
            "value": "C"
          }
        ],
        "type": "or"
      },
      "type": "not"
    });
  });

  it.skip("works on fancy filter (IN IS)", () => {
    var ex = $('diamonds').filter("$color.in(['D', 'C']) == true");

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({

    });
  });

  it("works with timePart (with limit)", () => {
    var ex = ply()
      .apply(
        'HoursOfDay',
        $("diamonds").split("$time.timePart(HOUR_OF_DAY, 'Etc/UTC')", 'HourOfDay')
          .apply('TotalPrice', '$diamonds.sum($price)')
          .sort('$HourOfDay', 'ascending')
          .limit(20)
      );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "fieldName": "price",
            "name": "TotalPrice",
            "type": "doubleSum"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "__time",
          "extractionFn": {
            "format": "H",
            "locale": "en-US",
            "timeZone": "Etc/UTC",
            "type": "timeFormat"
          },
          "outputName": "HourOfDay",
          "type": "extraction"
        },
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "metric": {
          "type": "alphaNumeric"
        },
        "queryType": "topN",
        "threshold": 20
      }
    ]);
  });

  it("works with timePart (no limit)", () => {
    var ex = ply()
      .apply(
        'HoursOfDay',
        $("diamonds").split("$time.timePart(HOUR_OF_DAY, 'Etc/UTC')", 'HourOfDay')
          .sort('$HourOfDay', 'ascending')
      )
      .apply(
        'SecondOfDay',
        $("diamonds").split("$time.timePart(SECOND_OF_DAY, 'Etc/UTC')", 'HourOfDay')
          .sort('$HourOfDay', 'ascending')
      );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "!DUMMY",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "__time",
          "extractionFn": {
            "format": "H",
            "locale": "en-US",
            "timeZone": "Etc/UTC",
            "type": "timeFormat"
          },
          "outputName": "HourOfDay",
          "type": "extraction"
        },
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "metric": {
          "type": "alphaNumeric"
        },
        "queryType": "topN",
        "threshold": 1000
      },
      {
        "aggregations": [
          {
            "name": "!DUMMY",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimensions": [
          {
            "dimension": "__time",
            "extractionFn": {
              "format": "H'*60+'m'*60+'s",
              "locale": "en-US",
              "timeZone": "Etc/UTC",
              "type": "timeFormat"
            },
            "outputName": "HourOfDay",
            "type": "extraction"
          }
        ],
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "limitSpec": {
          "columns": [
            {
              "dimension": "HourOfDay",
              "dimensionOrder": "alphaNumeric",
              "direction": "ascending"
            }
          ],
          "limit": 500000,
          "type": "default"
        },
        "queryType": "groupBy"
      }
    ]);
  });

  it("works with basic concat", () => {
    var ex = ply()
      .apply(
        'Colors',
        $("diamonds").split("'!!!<' ++ $color ++ '>!!!'", 'Colors')
          .apply('TotalPrice', '$diamonds.sum($price)')
          .sort('$TotalPrice', 'descending')
          .limit(3)
      );

    expect(ex.simulateQueryPlan(context)[0].dimension).to.deep.equal({
      "dimension": "color",
      "extractionFn": {
        "function": "function(d){return ((\"!!!<\"+d)+\">!!!\");}",
        "type": "javascript",
        "injective": true
      },
      "outputName": "Colors",
      "type": "extraction"
    });
  });

  it("works with basic substr", () => {
    var ex = ply()
      .apply(
        'Colors',
        $("diamonds").split("$color.substr(1, 2)", 'Colors')
          .apply('TotalPrice', '$diamonds.sum($price)')
          .sort('$TotalPrice', 'descending')
          .limit(3)
      );

    expect(ex.simulateQueryPlan(context)[0].dimension).to.deep.equal({
      "dimension": "color",
      "extractionFn": {
        "type": "substring",
        "index": 1,
        "length": 2
      },
      "outputName": "Colors",
      "type": "extraction"
    });
  });

  it("works with basic boolean split", () => {
    var ex = ply()
      .apply(
        'Colors',
        $("diamonds").split("$color == A", 'IsA')
          .apply('TotalPrice', '$diamonds.sum($price)')
          .sort('$TotalPrice', 'descending')
          .limit(10)
      );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "fieldName": "price",
            "name": "TotalPrice",
            "type": "doubleSum"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "color",
          "extractionFn": {
            "function": "function(d){return (d===\"A\");}",
            "type": "javascript"
          },
          "outputName": "IsA",
          "type": "extraction"
        },
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "metric": "TotalPrice",
        "queryType": "topN",
        "threshold": 10
      }
    ]);
  });

  it("works with having filter", () => {
    var ex = $("diamonds").split("$cut", 'Cut')
      .apply('Count', $('diamonds').count())
      .sort('$Count', 'descending')
      .filter($('Count').greaterThan(100))
      .limit(10);

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimensions": [
          {
            "dimension": "cut",
            "outputName": "Cut",
            "type": "default"
          }
        ],
        "granularity": "all",
        "having": {
          "aggregation": "Count",
          "type": "greaterThan",
          "value": 100
        },
        "intervals": "2015-03-12/2015-03-19",
        "limitSpec": {
          "columns": [
            {
              "dimension": "Count",
              "direction": "descending"
            }
          ],
          "limit": 10,
          "type": "default"
        },
        "queryType": "groupBy"
      }
    ]);
  });

  it("works with lower bound only time filter", () => {
    var ex = ply()
      .apply('diamonds', $("diamonds").filter($("time").in({ start: new Date('2015-03-12T00:00:00'), end: null })))
      .apply('Count', $('diamonds').count());

    expect(ex.simulateQueryPlan(contextUnfiltered)[0].intervals).to.equal("2015-03-12/3000-01-01");
  });

  it("works with upper bound only time filter", () => {
    var ex = ply()
      .apply('diamonds', $("diamonds").filter($("time").in({ start: null, end: new Date('2015-03-12T00:00:00') })))
      .apply('Count', $('diamonds').count());

    expect(ex.simulateQueryPlan(contextUnfiltered)[0].intervals).to.equal("1000-01-01/2015-03-12");
  });

  it("works with numeric split", () => {
    var ex = ply()
      .apply(
        'CaratSplit',
        $("diamonds").split("$carat", 'Carat')
          .sort('$Carat', 'descending')
          .limit(10)
      );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "!DUMMY",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "carat",
          "outputName": "Carat",
          "type": "default"
        },
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "metric": {
          "metric": {
            "type": "alphaNumeric"
          },
          "type": "inverted"
        },
        "queryType": "topN",
        "threshold": 10
      }
    ]);
  });

  it("works with set filter and split (and subsplit)", () => {
    var ex = ply()
      .apply("diamonds", $("diamonds").filter('$tags.overlap(["tagA", "tagB"])'))
      .apply(
        'Tags',
        $("diamonds").split("$tags", 'Tag')
          .sort('$Tag', 'descending')
          .limit(10)
          .apply(
            'Cuts',
            $("diamonds").split("$cut", 'Cut')
              .apply('Count', $('diamonds').count())
              .sort('$Count', 'descending')
              .limit(10)
          )
      );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "!DUMMY",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "tags",
          "outputName": "Tag",
          "type": "default"
        },
        "filter": {
          "fields": [
            {
              "dimension": "tags",
              "type": "selector",
              "value": "tagA"
            },
            {
              "dimension": "tags",
              "type": "selector",
              "value": "tagB"
            }
          ],
          "type": "or"
        },
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "metric": {
          "metric": {
            "type": "lexicographic"
          },
          "type": "inverted"
        },
        "queryType": "topN",
        "threshold": 10
      },
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "cut",
          "outputName": "Cut",
          "type": "default"
        },
        "filter": {
          "fields": [
            {
              "fields": [
                {
                  "dimension": "tags",
                  "type": "selector",
                  "value": "tagA"
                },
                {
                  "dimension": "tags",
                  "type": "selector",
                  "value": "tagB"
                }
              ],
              "type": "or"
            },
            {
              "dimension": "tags",
              "type": "selector",
              "value": "some_tags"
            }
          ],
          "type": "and"
        },
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "metric": "Count",
        "queryType": "topN",
        "threshold": 10
      }
    ]);
  });

  it("works with BOOLEAN split", () => {
    var ex = $("diamonds").split("$isNice", 'IsNice')
      .apply('Count', $('diamonds').count())
      .sort('$Count', 'descending');

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "isNice",
          "extractionFn": {
            "lookup": {
              "map": {
                "0": "false",
                "1": "true",
                "false": "false",
                "true": "true"
              },
              "type": "map"
            },
            "type": "lookup"
          },
          "outputName": "IsNice",
          "type": "extraction"
        },
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "metric": "Count",
        "queryType": "topN",
        "threshold": 1000
      }
    ]);
  });

  it("works with range bucket", () => {
    var ex = ply()
      .apply(
        'HeightBuckets',
        $("diamonds").split("$height_bucket", 'HeightBucket')
          .apply('Count', $('diamonds').count())
          .sort('$Count', 'descending')
          .limit(10)
      )
      .apply(
        'HeightUpBuckets',
        $("diamonds").split($('height_bucket').numberBucket(2, 0.5), 'HeightBucket')
          .apply('Count', $('diamonds').count())
          .sort('$Count', 'descending')
          .limit(10)
      );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "extractionFn": {
            "function": "function(d) {\nvar m = d.match(/^((?:-?[1-9]\\d*|0)\\.\\d{2});((?:-?[1-9]\\d*|0)\\.\\d{2})$/);\nif(!m) return 'null';\nvar s = +m[1];\nif(!(Math.abs(+m[2] - s - 0.05) < 1e-6)) return 'null'; \nvar parts = String(Math.abs(s)).split('.');\nparts[0] = ('000000000' + parts[0]).substr(-10);\nreturn (start < 0 ?'-':'') + parts.join('.');\n}",
            "type": "javascript"
          },
          "dimension": "height_bucket",
          "outputName": "HeightBucket",
          "type": "extraction"
        },
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "metric": "Count",
        "queryType": "topN",
        "threshold": 10
      },
// ---------------------
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "extractionFn": {
            "function": "function(d) {\nvar m = d.match(/^((?:-?[1-9]\\d*|0)\\.\\d{2});((?:-?[1-9]\\d*|0)\\.\\d{2})$/);\nif(!m) return 'null';\nvar s = +m[1];\nif(!(Math.abs(+m[2] - s - 0.05) < 1e-6)) return 'null'; s=Math.floor((s - 0.5) / 2) * 2 + 0.5;\nvar parts = String(Math.abs(s)).split('.');\nparts[0] = ('000000000' + parts[0]).substr(-10);\nreturn (start < 0 ?'-':'') + parts.join('.');\n}",
            "type": "javascript"
          },
          "dimension": "height_bucket",
          "outputName": "HeightBucket",
          "type": "extraction"
        },
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "metric": "Count",
        "queryType": "topN",
        "threshold": 10
      }
    ]);
  });

  it("makes a timeBoundary query", () => {
    var ex = ply()
      .apply('maximumTime', '$diamonds.max($time)')
      .apply('minimumTime', '$diamonds.min($time)');

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "dataSource": "diamonds",
        "queryType": "timeBoundary"
      }
    ]);
  });

  it("makes a timeBoundary query (maxTime only)", () => {
    var ex = ply()
      .apply('maximumTime', '$diamonds.max($time)');

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "dataSource": "diamonds",
        "queryType": "timeBoundary",
        "bound": "maxTime"
      }
    ]);
  });

  it("makes a timeBoundary query (minTime only)", () => {
    var ex = ply()
      .apply('minimumTime', '$diamonds.min($time)');

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "dataSource": "diamonds",
        "queryType": "timeBoundary",
        "bound": "minTime"
      }
    ]);
  });

  it("makes a topN with a timePart dim extraction fn", () => {
    var ex = $("diamonds").split($("time").timePart('SECOND_OF_DAY', 'Etc/UTC'), 'Time')
      .apply('Count', $('diamonds').count())
      .sort('$Count', 'descending')
      .limit(10);

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "__time",
          "extractionFn": {
            "format": "H'*60+'m'*60+'s",
            "locale": "en-US",
            "timeZone": "Etc/UTC",
            "type": "timeFormat"
          },
          "outputName": "Time",
          "type": "extraction"
        },
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "metric": "Count",
        "queryType": "topN",
        "threshold": 10
      }
    ]);
  });

  it("makes a filtered aggregate query", () => {
    var ex = ply()
      .apply(
        'BySegment',
        $('diamonds').split($("time").timeBucket('PT1H', 'Etc/UTC'), 'TimeSegment')
          .apply('Total', $('diamonds').sum('$price'))
          .apply('GoodPrice', $('diamonds').filter($('cut').is('Good')).sum('$price'))
          .apply('GoodPrice2', $('diamonds').filter($('cut').is('Good')).sum('$price.power(2)'))
          .apply('GoodishPrice', $('diamonds').filter($('cut').contains('Good')).sum('$price'))
          .apply('NotBadColors', $('diamonds').filter($('cut').isnt('Bad')).countDistinct('$color'))
      );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "fieldName": "price",
            "name": "Total",
            "type": "doubleSum"
          },
          {
            "aggregator": {
              "fieldName": "price",
              "name": "GoodPrice",
              "type": "doubleSum"
            },
            "filter": {
              "dimension": "cut",
              "type": "selector",
              "value": "Good"
            },
            "name": "GoodPrice",
            "type": "filtered"
          },
          {
            "aggregator": {
              "fieldNames": [
                "price"
              ],
              "fnAggregate": "function(_c,price) { return _c+Math.pow(price,2); }",
              "fnCombine": "function(a,b) { return a+b; }",
              "fnReset": "function() { return 0; }",
              "name": "GoodPrice2",
              "type": "javascript"
            },
            "filter": {
              "dimension": "cut",
              "type": "selector",
              "value": "Good"
            },
            "name": "GoodPrice2",
            "type": "filtered"
          },
          {
            "aggregator": {
              "fieldName": "price",
              "name": "GoodishPrice",
              "type": "doubleSum"
            },
            "filter": {
              "dimension": "cut",
              "extractionFn": {
                "function": "function(d){return (''+d).indexOf(\"Good\")>-1;}",
                "type": "javascript"
              },
              "type": "extraction",
              "value": "true"
            },
            "name": "GoodishPrice",
            "type": "filtered"
          },
          {
            "aggregator": {
              "byRow": true,
              "fieldNames": [
                "color"
              ],
              "name": "NotBadColors",
              "type": "cardinality"
            },
            "filter": {
              "field": {
                "dimension": "cut",
                "type": "selector",
                "value": "Bad"
              },
              "type": "not"
            },
            "name": "NotBadColors",
            "type": "filtered"
          }
        ],
        "dataSource": "diamonds",
        "granularity": {
          "period": "PT1H",
          "timeZone": "Etc/UTC",
          "type": "period"
        },
        "intervals": "2015-03-12/2015-03-19",
        "queryType": "timeseries",
        "context": {
          "skipEmptyBuckets": "true"
        }
      }
    ]);
  });

  it.skip("makes a filter on timePart", () => {
    var ex = $("diamonds").filter(
      $("time").timePart('HOUR_OF_DAY', 'Etc/UTC').in([3, 4, 10]).and($("time").in([
        TimeRange.fromJS({ start: new Date('2015-03-12T00:00:00'), end: new Date('2015-03-15T00:00:00') }),
        TimeRange.fromJS({ start: new Date('2015-03-16T00:00:00'), end: new Date('2015-03-18T00:00:00') })
      ]))
    ).split("$color", 'Color')
      .apply('Count', $('diamonds').count())
      .sort('$Count', 'descending')
      .limit(10);

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "color",
          "outputName": "Color",
          "type": "default"
        },
        "granularity": "all",
        "intervals": [
          "2015-03-12T03/2015-03-12T05",
          "2015-03-12T10/2015-03-12T11",
          "2015-03-13T03/2015-03-13T05",
          "2015-03-13T10/2015-03-13T11",
          "2015-03-14T03/2015-03-14T05",
          "2015-03-14T10/2015-03-14T11",

          "2015-03-16T03/2015-03-16T05",
          "2015-03-16T10/2015-03-16T11",
          "2015-03-17T03/2015-03-17T05",
          "2015-03-17T10/2015-03-17T11"
        ],
        "metric": "Count",
        "queryType": "topN",
        "threshold": 10
      }
    ]);
  });

  it.skip("splits on timePart with sub split", () => {
    var ex = $("diamonds").split($("time").timePart('HOUR_OF_DAY', 'Etc/UTC'), 'hourOfDay')
      .apply('Count', '$diamonds.count()')
      .sort('$Count', 'descending')
      .limit(3)
      .apply(
        'Colors',
        $("diamonds").split("$color", 'Color')
          .apply('Count', $('diamonds').count())
          .sort('$Count', 'descending')
          .limit(10)
      );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "__time",
          "extractionFn": {
            "format": "H",
            "locale": "en-US",
            "timeZone": "Etc/UTC",
            "type": "timeFormat"
          },
          "outputName": "hourOfDay",
          "type": "extraction"
        },
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "metric": "Count",
        "queryType": "topN",
        "threshold": 3
      },
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "color",
          "outputName": "Color",
          "type": "default"
        },
        "granularity": "all",
        "intervals": [
          "2015-03-12T04/2015-03-12T05",
          "2015-03-13T04/2015-03-13T05",
          "2015-03-14T04/2015-03-14T05",
          "2015-03-15T04/2015-03-15T05",
          "2015-03-16T04/2015-03-16T05",
          "2015-03-17T04/2015-03-17T05",
          "2015-03-18T04/2015-03-18T05"
        ],
        "metric": "Count",
        "queryType": "topN",
        "threshold": 10
      }
    ]);
  });

  it("works without a sort defined", () => {
    var ex = ply()
      .apply(
        'topN',
        $("diamonds").split("$color", 'Color')
          .apply('Count', $('diamonds').count())
          .limit(10)
      )
      .apply(
        'timeseries',
        $("diamonds").split($("time").timeBucket('P1D', 'America/Los_Angeles'), 'Timestamp')
          .apply('Count', $('diamonds').count())
          .limit(10)
      );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "color",
          "outputName": "Color",
          "type": "default"
        },
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "metric": {
          "type": "lexicographic"
        },
        "queryType": "topN",
        "threshold": 10
      },
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "granularity": {
          "period": "P1D",
          "timeZone": "America/Los_Angeles",
          "type": "period"
        },
        "intervals": "2015-03-12/2015-03-19",
        "queryType": "timeseries",
        "context": {
          "skipEmptyBuckets": "true"
        }
      }
    ]);
  });

  it("works with no attributes in dimension split dataset", () => {
    var ex = ply()
      .apply(
        'Cuts',
        $('diamonds').split("$cut", 'Cut')
          .sort('$Cut', 'ascending')
          .limit(5)
          .apply(
            'Colors',
            $('diamonds').split('$color', 'Color')
              .apply('Count', $('diamonds').count())
              .sort('$Count', 'descending')
              .limit(3)
          )
      );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "!DUMMY",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "cut",
          "outputName": "Cut",
          "type": "default"
        },
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "metric": {
          "type": "lexicographic"
        },
        "queryType": "topN",
        "threshold": 5
      },
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "color",
          "outputName": "Color",
          "type": "default"
        },
        "filter": {
          "dimension": "cut",
          "type": "selector",
          "value": "some_cut"
        },
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "metric": "Count",
        "queryType": "topN",
        "threshold": 3
      }
    ]);
  });

  it("works with no attributes in time split dataset", () => {
    var ex = ply()
      .apply(
        'ByHour',
        $('diamonds').split($("time").timeBucket('PT1H', 'Etc/UTC'), 'TimeByHour')
          .sort('$TimeByHour', 'ascending')
          .apply(
            'Colors',
            $('diamonds').split('$color', 'Color')
              .apply('Count', $('diamonds').count())
              .sort('$Count', 'descending')
              .limit(3)
          )
      );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "!DUMMY",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "granularity": {
          "period": "PT1H",
          "timeZone": "Etc/UTC",
          "type": "period"
        },
        "intervals": "2015-03-12/2015-03-19",
        "queryType": "timeseries",
        "context": {
          "skipEmptyBuckets": "true"
        }
      },
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "color",
          "outputName": "Color",
          "type": "default"
        },
        "granularity": "all",
        "intervals": "2015-03-14/2015-03-14T01",
        "metric": "Count",
        "queryType": "topN",
        "threshold": 3
      }
    ]);
  });

  it.skip("inlines a defined derived attribute", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').apply('sale_price', '$price + $tax'))
      .apply(
        'ByTime',
        $('diamonds').split($("time").timeBucket('P1D', 'Etc/UTC'), 'Time')
          .apply('TotalSalePrice', $('diamonds').sum('$sale_price'))
      );

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "fieldName": "price",
            "name": "!T_0",
            "type": "doubleSum"
          },
          {
            "fieldName": "tax",
            "name": "!T_1",
            "type": "doubleSum"
          }
        ],
        "dataSource": "diamonds",
        "granularity": {
          "period": "P1D",
          "timeZone": "Etc/UTC",
          "type": "period"
        },
        "intervals": "2015-03-12/2015-03-19",
        "postAggregations": [
          {
            "fields": [
              {
                "fieldName": "!T_0",
                "type": "fieldAccess"
              },
              {
                "fieldName": "!T_1",
                "type": "fieldAccess"
              }
            ],
            "fn": "+",
            "name": "TotalSalePrice",
            "type": "arithmetic"
          }
        ],
        "queryType": "timeseries",
        "context": {
          "skipEmptyBuckets": "true"
        }
      }
    ]);
  });

  it("makes a query on a dataset with a fancy name", () => {
    var ex = ply()
      .apply('maximumTime', '${diamonds-alt:;<>}.max($time)')
      .apply('minimumTime', '${diamonds-alt:;<>}.min($time)');

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "dataSource": "diamonds-alt:;<>",
        "queryType": "timeBoundary"
      }
    ]);
  });

  it("makes a query with countDistinct", () => {
    var ex = ply()
      .apply('NumColors', '$diamonds.countDistinct($color)')
      .apply('NumVendors', '$diamonds.countDistinct($vendor_id)')
      .apply('VendorsByColors', '$NumVendors / $NumColors');

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "byRow": true,
            "fieldNames": [
              "color"
            ],
            "name": "NumColors",
            "type": "cardinality"
          },
          {
            "fieldName": "vendor_id",
            "name": "NumVendors",
            "type": "hyperUnique"
          }
        ],
        "dataSource": "diamonds",
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "postAggregations": [
          {
            "fields": [
              {
                "fieldName": "NumVendors",
                "type": "hyperUniqueCardinality"
              },
              {
                "fieldName": "NumColors",
                "type": "hyperUniqueCardinality"
              }
            ],
            "fn": "/",
            "name": "VendorsByColors",
            "type": "arithmetic"
          }
        ],
        "queryType": "timeseries"
      }
    ]);
  });

  it("works with duplicate aggregates", () => {
    var ex = ply()
      .apply('Price', '$diamonds.sum($price)')
      .apply('Price', '$diamonds.sum($price)')
      .apply('M', '$diamonds.max($price)')
      .apply('M', '$diamonds.min($price)')
      .apply('Post', '$diamonds.count() * 2')
      .apply('Post', '$diamonds.count() * 3');

    expect(ex.simulateQueryPlan(context)).to.deep.equal([
      {
        "aggregations": [
          {
            "fieldName": "price",
            "name": "Price",
            "type": "doubleSum"
          },
          {
            "fieldName": "price",
            "name": "M",
            "type": "doubleMin"
          },
          {
            "name": "!T_0",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "postAggregations": [
          {
            "fields": [
              {
                "fieldName": "!T_0",
                "type": "fieldAccess"
              },
              {
                "type": "constant",
                "value": 3
              }
            ],
            "fn": "*",
            "name": "Post",
            "type": "arithmetic"
          }
        ],
        "queryType": "timeseries"
      }
    ]);
  });

  it("works on exact time filter (is)", () => {
    var ex = ply()
      .apply('diamonds', $('diamonds').filter($('time').is(new Date('2015-03-12T01:00:00.123Z'))))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].intervals).to.equal(
      "2015-03-12T01:00:00.123/2015-03-12T01:00:00.124"
    );
  });

  it("works on exact time filter (in interval)", () => {
    var ex = ply()
      .apply('diamonds', $('diamonds').filter($('time').in(new Date('2015-03-12T01:00:00.123Z'), new Date('2015-03-12T01:00:00.124Z'))))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].intervals).to.equal(
      "2015-03-12T01:00:00.123/2015-03-12T01:00:00.124"
    );
  });

  it("works contains filter (case sensitive)", () => {
    var ex = ply()
      .apply('diamonds', $('diamonds').filter($('color').contains(r('sup"yo'))))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "function": "function(d){return (''+d).indexOf(\"sup\\\"yo\")>-1;}",
      "type": "javascript"
    });
  });

  it("works contains filter (case insensitive)", () => {
    var ex = ply()
      .apply('diamonds', $('diamonds').filter($('color').contains(r('sup"yo'), 'ignoreCase')))
      .apply('Count', '$diamonds.count()');

    expect(ex.simulateQueryPlan(context)[0].filter).to.deep.equal({
      "dimension": "color",
      "query": {
        "type": "fragment",
        "values": ['sup"yo']
      },
      "type": "search"
    });
  });

  it("works with SELECT query", () => {
    var ex = $('diamonds')
      .filter('$color == "D"')
      .limit(10);

    var queryPlan = ex.simulateQueryPlan(context);

    expect(queryPlan).to.deep.equal([
      {
        "dataSource": "diamonds",
        "dimensions": [
          "color",
          "cut",
          "isNice",
          "tags",
          "carat",
          "height_bucket"
        ],
        "filter": {
          "dimension": "color",
          "type": "selector",
          "value": "D"
        },
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "metrics": [
          "price",
          "tax",
          "vendor_id"
        ],
        "pagingSpec": {
          "pagingIdentifiers": {},
          "threshold": 10
        },
        "queryType": "select"
      }
    ]);
  });

  it("works with single split expression", () => {
    var ex = $("diamonds").split("$cut", 'Cut');

    var queryPlan = ex.simulateQueryPlan(context);

    expect(queryPlan).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "!DUMMY",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimensions": [
          {
            "dimension": "cut",
            "outputName": "Cut",
            "type": "default"
          }
        ],
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "limitSpec": {
          "columns": [
            "Cut"
          ],
          "limit": 500000,
          "type": "default"
        },
        "queryType": "groupBy"
      }
    ]);
  });

  it("works multi-dimensional GROUP BYs", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter($("color").in(['A', 'B', 'some_color'])))
      .apply(
        'Cuts',
        $("diamonds").split({
            'Cut': "$cut",
            'Color': '$color',
            'TimeByHour': '$time.timeBucket(PT1H, "Etc/UTC")'
          })
          .apply('Count', $('diamonds').count())
          .limit(3)
          .apply(
            'Carats',
            $("diamonds").split($("carat").numberBucket(0.25), 'Carat')
              .apply('Count', $('diamonds').count())
              .sort('$Count', 'descending')
              .limit(3)
          )
      );

    var queryPlan = ex.simulateQueryPlan(context);

    expect(queryPlan).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimensions": [
          {
            "dimension": "color",
            "outputName": "Color",
            "type": "default"
          },
          {
            "dimension": "cut",
            "outputName": "Cut",
            "type": "default"
          },
          {
            "dimension": "__time",
            "extractionFn": {
              "format": "yyyy-MM-dd'T'HH':00Z",
              "locale": "en-US",
              "timeZone": "Etc/UTC",
              "type": "timeFormat"
            },
            "outputName": "TimeByHour",
            "type": "extraction"
          }
        ],
        "filter": {
          "fields": [
            {
              "dimension": "color",
              "type": "selector",
              "value": "A"
            },
            {
              "dimension": "color",
              "type": "selector",
              "value": "B"
            },
            {
              "dimension": "color",
              "type": "selector",
              "value": "some_color"
            }
          ],
          "type": "or"
        },
        "granularity": "all",
        "intervals": "2015-03-12/2015-03-19",
        "limitSpec": {
          "columns": [
            "Color"
          ],
          "limit": 3,
          "type": "default"
        },
        "queryType": "groupBy"
      },
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimension": {
          "dimension": "carat",
          "extractionFn": {
            "function": "function(d){d=Number(d); if(isNaN(d)) return 'null'; return Math.floor(d / 0.25) * 0.25;}",
            "type": "javascript"
          },
          "outputName": "Carat",
          "type": "extraction"
        },
        "filter": {
          "fields": [
            {
              "dimension": "color",
              "type": "selector",
              "value": "some_color"
            },
            {
              "dimension": "cut",
              "type": "selector",
              "value": "some_cut"
            }
          ],
          "type": "and"
        },
        "granularity": "all",
        "intervals": "2015-03-14/2015-03-14T01",
        "metric": "Count",
        "queryType": "topN",
        "threshold": 3
      }
    ]);
  });

  it("works multi-dimensional GROUP BYs (no limit)", () => {
    var ex = ply()
      .apply("diamonds", $('diamonds').filter($("color").in(['A', 'B', 'some_color'])))
      .apply(
        'Cuts',
        $("diamonds").split({
            'Cut': "$cut",
            'Color': '$color',
            'TimeByHour': '$time.timeBucket(PT1H, "Etc/UTC")'
          })
          .apply('Count', $('diamonds').count())
      );

    var queryPlan = ex.simulateQueryPlan(context);

    expect(queryPlan).to.deep.equal([
      {
        "aggregations": [
          {
            "name": "Count",
            "type": "count"
          }
        ],
        "dataSource": "diamonds",
        "dimensions": [
          {
            "dimension": "color",
            "outputName": "Color",
            "type": "default"
          },
          {
            "dimension": "cut",
            "outputName": "Cut",
            "type": "default"
          }
        ],
        "filter": {
          "fields": [
            {
              "dimension": "color",
              "type": "selector",
              "value": "A"
            },
            {
              "dimension": "color",
              "type": "selector",
              "value": "B"
            },
            {
              "dimension": "color",
              "type": "selector",
              "value": "some_color"
            }
          ],
          "type": "or"
        },
        "granularity": {
          "period": "PT1H",
          "timeZone": "Etc/UTC",
          "type": "period"
        },
        "intervals": "2015-03-12/2015-03-19",
        "limitSpec": {
          "columns": [
            "Color"
          ],
          "limit": 500000,
          "type": "default"
        },
        "queryType": "groupBy"
      }
    ]);
  });

  it("adds context to query if set on External", (testComplete) => {
    var ds = External.fromJS({
      engine: 'druid',
      dataSource: 'diamonds',
      timeAttribute: 'time',
      attributes,
      allowSelectQueries: true,
      filter: $("time").in({
        start: new Date('2015-03-12T00:00:00'),
        end: new Date('2015-03-19T00:00:00')
      }),
      context: {
        priority: -1,
        queryId: 'test'
      }
    });

    var ex = ply()
      .apply("diamonds", $('diamonds').filter($("color").is('D')))
      .apply('Count', '$diamonds.count()')
      .apply('TotalPrice', '$diamonds.sum($price)');

    expect(ex.simulateQueryPlan({ diamonds: ds })[0].context).to.deep.equal({ priority: -1, queryId: 'test' });

    testComplete();
  });
});