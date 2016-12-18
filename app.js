var YQL = require('yql');

var query = new YQL('select * from yahoo.finance.quotes where symbol in ("YHOO","AAPL","GOOG","MSFT")');

query.exec(function(err, data) {
    if(err) {
        console.log('Error:', err);
    }
    console.log('Data returned:', JSON.stringify(data, null, 2));
});

//Example with ticker: GOOGL
var request = require('request');
var requestOptions = {
    'url': 'https://api.tiingo.com/tiingo/daily/googl/prices',
    'headers': {
        'Content-Type': 'application/json',
        'Authorization': auth
    }
};

request(requestOptions,
    function(error, response, body) {
        console.log(body);
    }
);