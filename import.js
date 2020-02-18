const config = require('config');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');
const indexName = config.get('elasticsearch.index_name');
const BULK_SIZE = 20000;
async function run () {
    // Create Elasticsearch client
    const client = new Client({ node: 'http://localhost:9200' });
    const body = await client.indices.exists({
      index: 'ruedb'
    })
    console.log(body.statusCode);
    if(body.statusCode == 200){
      console.log("suppression");
      await client.indices.delete({
        index: 'ruedb'
    });
    }
  
    await client.indices.create(
      { index: 'ruedb', 
      
        body: {
          mappings:{
            properties: {
              location: {
                type: "geo_point"
              }
            }
          } 
        }
      }
        , (err, resp) => {
        if (err) console.trace(err.message);
      });
   
    // Read CSV file
    let problemes = [];
    let compteur = 0;
    let nbr = 0;
    fs.createReadStream('dataset/dans-ma-rue.csv')
        .pipe(csv({
            separator: ';'
        }))
        .on('data', (data) => {
            problemes.push(
                {

                    "timestamp": data.DATEDECL,
                    "object_id": data.OBJECTID,
                    "annee_declaration": data["ANNEE DECLARATION"],
                    "mois_declaration": data["MOIS DECLARATION"],
                    "type": data.TYPE,
                    "sous_type": data.SOUSTYPE,
                    "code_postal": data.CODE_POSTAL,
                    "ville": data.VILLE,
                    "arrondissement": data.ARRONDISSEMENT,
                    "prefixe": data.PREFIXE,
                    "intervenant": data.INTERVENANT,
                    "conseil_de_quartier": data["CONSEIL DE QUARTIER"],
                  
                }

            )
            
            if(problemes.length >= BULK_SIZE){
              nbr+=20000;
                const tmp = [...problemes]
                setTimeout(() => {
                
                client.bulk(createBulkInsertQuery(tmp), (err, resp) => {
                  
                  console.log(resp.statusCode);
                    if (err) console.trace(err.message);
                    
                    else console.log(`Inserted ${resp.body.items.length} actors`);
                    
                  });
                }, 1000*compteur+1000);
                console.log('Terminated!');
                problemes = [];
                compteur+=1;
            }
            
        })
        .on('end', () => {
          nbr+=problemes.length;
          setTimeout(() => {
            client.bulk(createBulkInsertQuery(problemes), (err, resp) => {
                if (err) console.trace(err.message);
                else console.log(`Inserted ${resp.body.items.length} actors`);
                setTimeout(()=> {
                  client.close();
                },3000
                );
                
              });
            console.log(nbr);
          }, 1000*compteur+5000);
        });
        
        
}

run().catch(console.error);
function createBulkInsertQuery(incidents) {
  const body = incidents.reduce((acc, incident) => {
    const { timestamp, 
      annee_declaration, 
      mois_declaration, 
      type,
      sous_type,
      code_postal,
      ville,
      arrondissement,
      prefixe,
      intervenant,
      conseil_de_quartier,
      location
  } = incident;
    acc.push({ index: { _index: 'dansmarue', _type: '_doc'} })
    acc.push({ timestamp, 
      annee_declaration, 
      mois_declaration, 
      type,
      sous_type,
      code_postal,
      ville,
      arrondissement,
      prefixe,
      intervenant,
      conseil_de_quartier,
      location })
    return acc
  }, []);

  return { body };
}
  

  function sleep(milliseconds) {
      console.log("sleep");
    const date = Date.now();
    let currentDate = null;
    do {
      currentDate = Date.now();
    } while (currentDate - date < milliseconds);
  }
  

  
