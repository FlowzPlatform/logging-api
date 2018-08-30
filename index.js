const { json, send, createError, sendError } = require('micro');
const { router, get, post } = require('microrouter')
const ClickHouse = require("@apla/clickhouse");
console.log(ClickHouse);

const hello = async(req, res) => {

    console.log('hello.......');

    send(res, 200, "hi..")
}


const logData = async(req, res) => {
    // console.log('enter in logData')
    await create(req, res, 1);
    // console.log('>>>', result)
    // console.log('created in logData')
    // Promise.resolve(temp).then((value) => {
    //   console.log('after create call')
    //   insert(req, res);
    // }).catch(err => {
    //   console.log('?>>>>>', err)
    // })

    // send(res, 200, result)
}

const create = async(req, res, isInsert=0) => {

      console.log('isInsert :: ', isInsert);

      const ch = new ClickHouse({ host: 'localhost', port: 8123});

      let body = await json(req)

      var TableName = body.schema_name;
      console.log('Create tablename ', TableName);

       const SchemaDetail = body.schema;

      let Fields = Object.keys(SchemaDetail);
      let Datatypes = Object.values(SchemaDetail);

      let datatypes = [];
      datatypes["string"] = "String";
      datatypes["int"] = "UInt64";
      datatypes["float"] = "Float64";



      let createQuery = "CREATE TABLE IF NOT EXISTS `" + TableName + "`";
      let string1 =   " `logDate` Date, `logDateTime` DateTime";

      Fields.forEach(function(element){

        // console.log(datatypes[SchemaDetail[element]]);
        string1 = string1 + ", `" + element +  "` " + datatypes[SchemaDetail[element]] ;
        //string1 = string1 + ", `" + element +  "` " + SchemaDetail[element] ;
        //console.log(element, SchemaDetail[element]);
      });

      createQuery = createQuery + " ( " + string1 + " ) " + "ENGINE = MergeTree(logDate, (logDate), 8192)" ;
      // console.log(createQuery);

      // START : To create Table logtable if not exists
      // let CreateQuery = "CREATE TABLE IF NOT EXISTS `logtable`  ( `logDate` Date, `logDateTime` DateTime, `ip` String, `location` String, `pagename` String, `username` String, `referrer` String ) ENGINE = MergeTree(logDate, (logDate), 8192)";

      //assert (!err, err);
      let response = ch.query(createQuery, { queryOptions: { database: 'default' } }, function(err, result) {
          if (err) {
              console.log("Create err :: ", err);
          } else {
              console.log("Create res :: ", result);
              console.log("============== ", isInsert);
              if(isInsert == 1)
              {
                console.log('insert data..');
                  let response = insert(req, res);
              }

            //response = response + "Create Completed";
            //console.log(response);
            return "response";
          }
          //done ();
      });
      // console.log('create completed')
      // Promise.resolve(queryDone).then((value) => {
      //   console.log('Table Created');
      // }).catch(err => {
      //   console.log('Error in promise', err);
      // })

      //console.log(">>>>>", response);
      send(res, 200, "Create completed...!!");
      // END : To create Table logtable if not exists
}

const insert = async(req, res) => {

    const ch = new ClickHouse({ host: 'localhost', port: 8123});

    let body = await json(req);


    var TableName = body.schema_name;

    console.log(TableName);
    const data = body.data;

  let colnames = Object.keys(data);
  let colvalues = Object.values(data);

  let date = new Date().toISOString().split('T')[0];
  //console.log(date);
  let datetime =  new Date().toISOString().split('T')[0] + " " + new Date().toLocaleTimeString() ;
  //console.log(datetime);

  //let InsertQuery = "INSERT INTO logtable VALUES ( '" + new Date().toISOString().split('T')[0] + "','" + datetime + "','"+ ip + "','"+ location + "','" + pagename + "','" + username + "','" + referrer + "')";

  let InsertQuery = "INSERT INTO " + TableName;

  let string3 =   " `logDate`, `logDateTime`";
  colnames.forEach(function(element){
    string3 = string3 + ", `" + element +  "` ";
  });

  let string4 =   "'" + date + "', '" + datetime + "'";
  colvalues.forEach(function(element){
    string4 = string4 + ", '" + element +  "' ";
  });

  InsertQuery = InsertQuery + " ( " + string3 + " ) " + "VALUES" + " ( " + string4 + " ) " ;
  // console.log(InsertQuery);




  ch.query(InsertQuery, { queryOptions: { database: 'default' } }, function(err, result) {
      if (err) {
          console.log("Insert err :: ", err);
      } else {
          console.log("Insert res :: ", result);
          // return { insert: true }
      }
  });
  //console.log('insert result', temp)
  // send(res, 200, result)
  return "Insert Completed";
}

const select = async(req, res) => {

    let ch = new ClickHouse({ host: 'localhost' });

    var stream = ch.query ("SELECT * from logtable3 FORMAT JSON");

    // or collect records yourself

  stream.on ('metadata', function (columns) {
      console.log(columns);
    });

  stream.on ('data', function (row) {
      //rows.push (row);
      console.log(row);
    });

    /*stream.on ('error', function (err) {
      // TODO: handler error
    }); */

    stream.on ('end', function () {
      // all rows are collected, let's verify count
      //assert (rows.length === stream.supplemental.rows);
      // how many rows in result are set without windowing:
      //console.log ('rows in result set', stream.supplemental.rows_before_limit_at_least);
    });

    //console.log(rows);
    //console.log(stream);

    send(res, 200, "hi select..")
}

const inserttest = async(req, res) => {

    const ch = new ClickHouse({ host: 'localhost', port: 8123});

    let body = await json(req);


    var TableName = body.data_type;

    if (TableName == "heartbeat"){
      console.log("Heartbeat dropped.");
      return "Heartbeat dropped.";
    }
    //console.log(TableName);

    let date = new Date().toISOString().split('T')[0];
    //console.log(date);
    let datetime =  new Date().toISOString().split('T')[0] + " " + new Date().toLocaleTimeString() ;
    //console.log(datetime);

  //let InsertQuery = "INSERT INTO logtable VALUES ( '" + new Date().toISOString().split('T')[0] + "','" + datetime + "','"+ ip + "','"+ location + "','" + pagename + "','" + username + "','" + referrer + "')";

  let InsertQuery = "INSERT INTO " + TableName;

  let string3 =   " `logDate`, `logDateTime`";

  var camera_id = body.camera_id;
  var best_plate_number = body.best_plate_number;
  var best_confidence = body.best_confidence;
  var travel_direction = body.travel_direction;
  var processing_time_ms = body.best_plate.processing_time_ms;
  var best_region = body.best_region;
  var vehicle_make_temp = body.vehicle.make;
  var vehicle_color_temp = body.vehicle.color;
  var vehicle_body_type_temp = body.vehicle.body_type;

  //console.log(camera_id, best_plate_number, best_confidence, travel_direction, processing_time_ms, best_region);
  //console.log('make : ',vehicle_make_temp);
  //console.log('color : ',vehicle_color_temp);
  //console.log('body_type : ',vehicle_body_type_temp);

  let conf_make = 0;
  let vehicle_make;
  vehicle_make_temp.forEach(function(element){
    if( element.confidence >= conf_make){
      vehicle_make = element.name;
      conf_make = element.confidence;
    }
  });
//  console.log(vehicle_make);

  let conf_color = 0;
  let vehicle_color;
  vehicle_color_temp.forEach(function(element){
    if( element.confidence >= conf_color){
      vehicle_color = element.name;
      conf_color = element.confidence;
    }
  });
//  console.log(vehicle_color);

  let conf_body = 0;
  let vehicle_body;
  vehicle_body_type_temp.forEach(function(element){
    if( element.confidence >= conf_body){
      vehicle_body = element.name;
      conf_body = element.confidence;
    }
  });
  //console.log(vehicle_make, vehicle_color, vehicle_body);

  let string4 =   "'" + date + "', '" + datetime + "'";

    string4 = string4 + ", '" + camera_id +  "' "+ ", '" + best_plate_number +  "' "+ ", '" + parseFloat(best_confidence) +  "' "+ ", '" + parseFloat(travel_direction) +  "' "+ ", '" + parseFloat(processing_time_ms) +  "' "+ ", '" + vehicle_make +  "' "+ ", '" + vehicle_color +  "' "+ ", '" + vehicle_body +  "', '" + best_region +  "' ";


  InsertQuery = InsertQuery + " VALUES" + " ( " + string4 + " ) " ;
  console.log(InsertQuery);




  ch.query(InsertQuery, { queryOptions: { database: 'default' } }, function(err, result) {
      if (err) {
          console.log("Insert err :: ", err);
      } else {
          console.log("Insert res :: ", result);
          // return { insert: true }
      }
  });
  //console.log('insert result', temp)
  // send(res, 200, result)
  return "Insert Completed";
}

module.exports = router(
    //post('/hellopost', hellopost),
    get('/hello/:who', hello),
    post('/logdata', logData),
    post('/select', select),
    post('/create', create),
    post('/insert', insert),
    post('/insert-test/', inserttest)
    //get('/*', notfound)
)
