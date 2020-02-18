const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.count = (client, from, to, callback) => {
    console.log(from)
    console.log(to)
    client.count({
        index:"dansmarue",
        body:{
            query:{
                range:{
                    "@timestamp":{
                        gte:from,//"2016-01-01",
                        lt: to//"2018-01-01"

                    }
                }
                
            }
        }
        }).then((resp)=>{ 
            callback({
                count: resp.body.count
            })
        })
}

exports.countAround = (client, lat, lon, radius, callback) => {
    client
        .count({
            index: 'dansmarue',
            body: {
                query: {
                    "bool": {
                        "filter": {
                            "geo_distance": {
                                "distance": radius,
                                "location": [lon, lat]
                            }
                        }
                    }
                }
            }
        })
        .then(resp => callback({
            count: resp.body.count
        }), err => console.error(err.meta.body.error));
}
