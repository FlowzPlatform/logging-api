const { json, send, createError, sendError } = require('micro');
const { router, get, post } = require('microrouter')
const ClickHouse = require("@apla/clickhouse");
const url = require('url');
const _= require('lodash');
const cors = require('micro-cors')();
const fn = require('./functions');
const fnats = require('./fnnats');

console.log(ClickHouse);

console.log('=============================');
console.log(fn);


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

const select = cors(async(req, res) => {

    var url_parts = url.parse(req.url, true);
    // console.log(url_parts);

    var query = url_parts.query;
    //console.log(query);

    var keys = Object.keys(query);
    var values = Object.values(query);
    //console.log(keys);
    //console.log(values);

    let ch = new ClickHouse({ host: 'localhost' });

    selectQuery = "SELECT * from alpr_group ";

    var string5 = " where ";

    if(Object.keys(query).length > 0){

      keys.forEach(function(element){
        string5 = string5 + element + " = " + query[element] + " AND ";
      });

      var pos = string5.lastIndexOf("AND")
      string5 = string5.slice(0, pos);
      selectQuery = selectQuery + string5;
      //console.log(string5);
    }

    selectQuery = selectQuery + " FORMAT JSON ";

    console.log(selectQuery);
    var stream = ch.query(selectQuery);


      // or collect records yourself
      let rows = [];

      /*  stream.on ('metadata', function (columns) {
         console.log(columns);
       }); */

        stream.on ('data', function (row) {
          rows.push(row);
          //console.log('inside loop' ,rows);
        });

        stream.on ('error', function (err) {
          //TODO: handler error
          console.log('select error :', err);
        });


        console.log('121212121212');
        await fetchData2(stream);
        console.log('aaaaaaaaaaaaaaaaaaaaaaa');


      // send(res, 200, " hi getdata...!!")
      send(res, 200, rows);
})

const fetchData2 = async(stream) => {
  console.log('2222222222')
  return new Promise(async(resolve, reject) => {
    await stream.on ('end', function () {
       // all rows are collected, let's verify count
      console.log('ccccccccccccccccc');
      resolve(1)
    });
  });
}


const select_distinct = cors(async(req, res) => {

    //console.log("into distinct");
    var url_parts = url.parse(req.url, true);
    // console.log(url_parts);

    var query = url_parts.query;
    //console.log(query);

    var value = query.column;
    //console.log(keys);
    console.log(value);

    let ch = new ClickHouse({ host: 'localhost' });

    selectQuery = "SELECT " + value + " from alpr_group group by " + value +" FORMAT JSON";

    console.log(selectQuery);
    var stream = ch.query(selectQuery);


      // or collect records yourself
      let rows = [];

      /*  stream.on ('metadata', function (columns) {
         console.log(columns);
       }); */

        stream.on ('data', function (row) {
          rows.push(row);
          //console.log('inside loop' ,rows);
        });

        stream.on ('error', function (err) {
          //TODO: handler error
          console.log('select error :', err);
        });


        await fetchData3(stream);
        console.log('getdata-distinct complete');


      // send(res, 200, " hi getdata...!!")
      send(res, 200, rows);
})

const fetchData3 = async(stream) => {
  return new Promise(async(resolve, reject) => {
    await stream.on ('end', function () {
       // all rows are collected, let's verify count
      resolve(1)
    });
  });
}

const inserttest = async(req, res) => {

    const ch = new ClickHouse({ host: 'localhost', port: 8123});

    let body = await json(req);


    var TableName = body.data_type;

    if (TableName == "heartbeat"){
      console.log("Heartbeat dropped.");
      return "Heartbeat dropped.";
    }
    else{};
    //console.log(TableName);

  var camera_id = body.camera_id;
  //console.log(typeof(camera_id));
  var company_id = body.company_id;
  var frame_start = body.frame_start;
  var frame_end = body.frame_end;
  var best_uuid = body.best_uuid;
  var best_plate_number = body.best_plate_number;
  var best_confidence = body.best_confidence;
  var best_region = body.best_region;
  //console.log(typeof(best_confidence));
  var travel_direction = body.travel_direction;


  var vehicle_make_temp = body.vehicle.make;
  var vehicle_make_model_temp = body.vehicle.make_model;
  var vehicle_color_temp = body.vehicle.color;
  var vehicle_body_type_temp = body.vehicle.body_type;
  var vehicle_year_temp = body.vehicle.year;


  var best_plate = body.best_plate;
  var coordinates = best_plate.coordinates;

  var x1 = coordinates[0].x;
  var y1 = coordinates[0].y;

  var x2 = coordinates[1].x;
  var y2 = coordinates[1].y;

  var x3 = coordinates[2].x;
  var y3 = coordinates[2].y;

  var x4 = coordinates[3].x;
  var y4 = coordinates[3].y;

  let is_parked;
  if (body.is_parked == false){
    is_parked = 0;
  }
  else{
    is_parked = 1;
  }

  //console.log(body.is_parked, is_parked);

  var vehicle_x = best_plate.vehicle_region.x;
  var vehicle_y = best_plate.vehicle_region.y;
  var vehicle_width = best_plate.vehicle_region.width;
  var vehicle_height = best_plate.vehicle_region.height;

  //console.log(camera_id, best_plate_number, best_confidence, travel_direction, processing_time_ms, best_region);
  //console.log('make : ',vehicle_make_temp);
  //console.log('color : ',vehicle_color_temp);
  //console.log('body_type : ',vehicle_body_type_temp);

  let conf_make = 0;
  let vehicle_make = "-";
  vehicle_make_temp.forEach(function(element){
    if( element.confidence >= conf_make){
      vehicle_make = element.name;
      conf_make = element.confidence;
    }
  });
  //  console.log(vehicle_make);

  let conf_make_model = 0;
  let vehicle_make_model = "-";
  vehicle_make_model_temp.forEach(function(element){
    if( element.confidence >= conf_make_model){
      vehicle_make_model = element.name;
      conf_make_model = element.confidence;
    }
  });
  //console.log(vehicle_make_model);

  let conf_color = 0;
  let vehicle_color = "-";
  vehicle_color_temp.forEach(function(element){
    if( element.confidence >= conf_color){
      vehicle_color = element.name;
      conf_color = element.confidence;
    }
  });
  //  console.log(vehicle_color);

  let conf_body = 0;
  let vehicle_body = "-";
  vehicle_body_type_temp.forEach(function(element){
    if( element.confidence >= conf_body){
      vehicle_body = element.name;
      conf_body = element.confidence;
    }
  });

  let conf_year = 0;
  let vehicle_year = "-";
  vehicle_year_temp.forEach(function(element){
    if( element.confidence >= conf_year){
      vehicle_year = element.name;
      conf_year = element.confidence;
    }
  });
  //console.log(vehicle_year);
  //console.log(vehicle_make, vehicle_color, vehicle_body);


  let date = new Date().toISOString().split('T')[0];
  //console.log(date);
  let datetime =  new Date().toISOString().split('T')[0] + " " + new Date().toLocaleTimeString() ;
  //console.log(datetime);
  let logType = "DEBUG";
  let logLevel = 1;
  //let InsertQuery = "INSERT INTO logtable VALUES ( '" + new Date().toISOString().split('T')[0] + "','" + datetime + "','"+ ip + "','"+ location + "','" + pagename + "','" + username + "','" + referrer + "')";

  let InsertQuery = "INSERT INTO " + TableName;

  let string3 =   " `logDate`, `logDateTime`";

  let string4 =   "'" + date + "', '" + datetime + "'";

  //string4 = string4 + ", " + camera_id +  " "+ ", '" + best_plate_number +  "' "+ ", " + best_confidence +  " "+ ", " + travel_direction +  " "+ ", " + processing_time_ms +  " "+ ", '" + vehicle_make +  "' "+ ", '" + vehicle_color +  "' "+ ", '" + vehicle_body +  "', '" + best_region +  "' ";
  string4 = string4 + " , '" + logType+ "' , " + logLevel + ", '" + camera_id + "' , '"+ company_id + "' , " + frame_start + " , " + frame_end + " , '" + best_uuid + "' , '" + best_plate_number +  "' , " + best_confidence + ", '" + best_region + "' , " + travel_direction + " , '" + vehicle_make +  "' , '" + vehicle_make_model + "' , '" + vehicle_color +  "' , '" + vehicle_body +  "' , '" + vehicle_year  +  "' , " + is_parked + "," + x1 + "," + y1+ "," + x2+ "," + y2+ "," + x3+ "," + y3+ "," + x4+ "," + y4 + "," + vehicle_x + "," + vehicle_y + "," + vehicle_width + "," + vehicle_height + " ";

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

const select_table = cors(async(req, res) => {

    //console.log(req.params);

    var tablename = req.params.tbl;

    let ch = new ClickHouse({ host: 'localhost' });

    selectQuery = "SELECT * from " + tablename + " FORMAT JSON" ;

    console.log(selectQuery);
    var stream = ch.query(selectQuery);


      // or collect records yourself
      let rows = [];

      /*  stream.on ('metadata', function (columns) {
         console.log(columns);
       }); */

        stream.on ('data', function (row) {
          rows.push(row);
          //console.log('inside loop' ,rows);
        });

        stream.on ('error', function (err) {
          //TODO: handler error
          console.log('select error :', err);
        });

        await fetchData2(stream);

      // send(res, 200, " hi getdata...!!")
      send(res, 200, rows);
})



module.exports = router(
    //post('/hellopost', hellopost),
    get('/hello/:who', hello),
    post('/logdata', logData),
    get('/getdata', select),
    get('/fetchdata/:tbl', select_table),
    get('/getdata-distinct', select_distinct),
    post('/create', create),
    post('/insert', insert),
    post('/insert-test/', inserttest),
    post('/publish-nats', fnats.publishNats)
    //get('/*', notfound)
)
