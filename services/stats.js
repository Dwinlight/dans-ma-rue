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
    client.search({
        index : "dansmarue",
        size: 0,
        body:{
            aggs : {
                "type" : {
                    "terms": {
                        "field": "type.keyword",
                        "order" : { "_count": "desc" }
                    },
                    aggs : {
                      "sous_type" : {
                        "terms": {
                          "field": "sous_type.keyword",
                          "order" : { "_count": "desc" }
                           }
                        }
                      }
                  
                }
            }
        }
            
    
    }).then((resp) =>{
        const tab =[];
        const aggr = resp.body.aggregations.type.buckets;
        for(let i =0; i<5;i++){
            let sousTab =[];
            const sous = aggr[i].sous_type.buckets
            for(let j=0; j<5;j++){
                sousTab.push({
                    "sous_type": sous[j]["key"],
                    "count": sous[j]["doc_count"]
                });}
            tab.push({
                "type": aggr[i]["key"],
                "count": aggr[i]["doc_count"],
                "sous_type": sousTab
            });
        }                
        
        callback(tab);
    }, err => console.error(err.meta.body.error));
}

exports.statsByMonth = (client, callback) => {
    client.search(
        { 
            index:indexName,
            body: { 
                aggs : { 
                    "anomalieParmois": { 
                        "date_histogram": { 
                            "field": "@timestamp",
                            "calendar_interval": "month",
                            "format": "MM/yyyy",
                            "order" : { "_count": "desc" }
                        }
                    }
                }
            }
        }
    ).then((resp)=>{ 
        let tab = []
        const temp = resp.body.aggregations.anomalieParmois.buckets;
        for(let i = 0;i<10;i++){
            tab.push({
                "date" : temp[i]["key_as_string"],
                "count": temp[i]["doc_count"]
            })
        }
        callback({
            body: tab
        })
    })
}

exports.statsPropreteByArrondissement = (client, callback) => {
    // TODO Trouver le top 3 des arrondissements avec le plus d'anomalies concernant la propreté
    callback([]);
}
