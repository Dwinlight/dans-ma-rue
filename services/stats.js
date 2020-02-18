const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.statsByArrondissement = (client, callback) => {
    client.search({
        index : "dansmarue",
        size: 0,
        body:{
            aggs : {
                "arrondissement" : {
                    "terms": {
                        "field": "arrondissement.keyword"
                    }
                }
        }
    }
    }).then((resp) =>{
        const tab =[];
        const aggr = resp.body.aggregations.arrondissement.buckets;
        
        aggr.forEach((element) => {
            tab.push({
                "arrondissement": element["key"],
                "count": element["doc_count"]
            });


        
    });
        callback(tab);
    }, err => console.error(err.meta.body.error));

}

exports.statsByType = (client, callback) => {
    // TODO Trouver le top 5 des types et sous types d'anomalies
    callback([]);
}

exports.statsByMonth = (client, callback) => {
    // TODO Trouver le top 10 des mois avec le plus d'anomalies
    callback([]);
}

exports.statsPropreteByArrondissement = (client, callback) => {
    // TODO Trouver le top 3 des arrondissements avec le plus d'anomalies concernant la propreté
    callback([]);
}
