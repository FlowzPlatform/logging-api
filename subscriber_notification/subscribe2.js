let NATS = require('nats');
const ClickHouse = require("@apla/clickhouse");
const cors = require('micro-cors')();

let servers = ['nats://172.16.61.20:4222'];
let nats = NATS.connect({'servers': servers});

const ch = new ClickHouse({ host: 'localhost', port: 8123});
let io = require('socket.io')(4085);

// currentServer is the URL of the connected server.
console.log("Connected to " + nats.currentServer.url.host);

let jsonque = []; //object to store 10 JSON

// Simple Subscriber
nats.subscribe('publish-data',async function(msg,res) {
      console.log('Received a message: ' );

      let body = JSON.parse(msg);

      var TableName = body.data_type;

      if (TableName == "heartbeat"){
        console.log("Heartbeat dropped.");
        return "Heartbeat dropped.";
      }
      else{

              if(jsonque.length < 9){
                jsonque.push(body);
                //console.log("#",jsonque.length);
              }
              else{
                jsonque.push(body);
                console.log(jsonque.length);
                //now jsonque length is 10. so we have to process que


                let last_10_plates = [];
                jsonque.forEach(function(element){
                  //console.log(element.best_plate_number);
                  last_10_plates.push(element.best_plate_number);
                });

                let list = "( ";
                last_10_plates.forEach(function(element){
                  list = list + "'" + element + "',";
                });

                var pos = list.lastIndexOf(",")
                list = list.slice(0, pos);
                list = list + " )";

                console.log(list);

                let result_rows;
                try{
                 result_rows = await select_que(list,res);
                 //console.log("plate found: " ,result_rows);
                }
                catch(error){
                  console.log("------->>",error);
                }

                let insertque = [];
                result_rows.forEach(function(element1){
                  jsonque.forEach(function(element2){
                      if(element1.plate == element2.best_plate_number){
                        insertque.push(element2);
                      }
                  });
                });
                console.log(insertque.length);

                insertque.forEach(async function(element){
                  let object= {};
                  try{
                    object = await  insert(element,res);

                       console.log("object",object);

                       console.log("------------------------------------");

                       io.emit('news', object);
                       console.log('emited', object);
                  }
                  catch(error){
                    console.log("Error In Insert (notification_history):",error);
                  }
                });
                jsonque = []; //at the end we have to do empty jsonque
              }
      };



        /*let result;
        try{
         result = await select(msg,res);
         console.log("plate found: " ,result);
        }
        catch(error){
          console.log("------->>",error);
        }

        if(result == "true"){
          console.log("Inserting Data Into notification_history");

              let object= {};
              try{
                object = await  insert(msg,res);

                   console.log("object",object);

                   console.log("------------------------------------");
                   // note, io(<port>) will create a http server for you

                   // io.on('connection', function (socket) {
                   //
                   //      //socket.emit('news', object);
                   //      console.log('object emited: ' , object);
                   //      socket.on('disconnect', function () {
                   //            io.emit('user disconnected');
                   //            console.log('disconnected');
                   //      });
                   // });

                     io.emit('news', object);
                     console.log('emited', object);

              }
              catch(error){
                console.log("Error In Insert (notification_history):",error);
              }
        } */
});


const select_que = async(list, res) => {

    selectQuery = "SELECT * from stolen_plate where plate in " + list + " FORMAT JSON";

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

      //console.log(rows);
      return rows;
}

const select = async(req, res) => {

    let body = JSON.parse(req);
    //console.log(body);

    var TableName = body.data_type;
    if (TableName == "heartbeat"){
      console.log("Heartbeat dropped.");
      return "Heartbeat dropped.";
    }
    else{};

    var plate_got = body.best_plate_number;
    console.log("plate_got: ", plate_got);

    selectQuery = "SELECT * from stolen_plate where plate='" + plate_got + "' FORMAT JSON";

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

      //send(res, 200, rows);
      console.log(rows);

      let result;
      if(rows.length <= 0){
        result = "false";
      }
      else{
        result = "true";
      }

      return result;
}

const fetchData2 = async(stream) => {
    //console.log('2222222222')
  return new Promise(async(resolve, reject) => {
    await stream.on ('end', function () {
       // all rows are collected, let's verify count
      //console.log('ccccccccccccccccc');
      resolve(1)
    });
  });
}

const select_location = async(camera_id) => {

    console.log("camera_id: ", camera_id);

    selectQuery = "SELECT location from sites where camera_id='" + camera_id + "' FORMAT JSON";

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

      //send(res, 200, rows);
      //console.log("location :" ,rows[0].location);

      return rows[0].location;
}

const insert = async(req, res) => {

    // let body = await json(req);
    //let body = JSON.parse(req);
    //console.log(req);
    let body = req;

    var plate_number = body.best_plate_number;
    var camera_id = body.camera_id;
    var type = "stolen";
    var company_id = body.company_id;
    var best_uuid = body.best_uuid;
    var best_region = body.best_region;
    var travel_direction = body.travel_direction;


    var vehicle_make_temp = body.vehicle.make;
    var vehicle_make_model_temp = body.vehicle.make_model;
    var vehicle_color_temp = body.vehicle.color;
    var vehicle_body_type_temp = body.vehicle.body_type;
    var vehicle_year_temp = body.vehicle.year;

    let is_parked;
    if (body.is_parked == false){
      is_parked = 0;
    }
    else{
      is_parked = 1;
    }


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


    //let date = new Date().toISOString().split('T')[0];
    //console.log(date);
    let datetime =  new Date().toISOString().split('T')[0] + " " + new Date().toLocaleTimeString() ;
    //console.log(datetime);
    //let InsertQuery = "INSERT INTO logtable VALUES ( '" + new Date().toISOString().split('T')[0] + "','" + datetime + "','"+ ip + "','"+ location + "','" + pagename + "','" + username + "','" + referrer + "')";

    let InsertQuery = "INSERT INTO notification_history ";

    //string4 = string4 + ", " + camera_id +  " "+ ", '" + best_plate_number +  "' "+ ", " + best_confidence +  " "+ ", " + travel_direction +  " "+ ", " + processing_time_ms +  " "+ ", '" + vehicle_make +  "' "+ ", '" + vehicle_color +  "' "+ ", '" + vehicle_body +  "', '" + best_region +  "' ";
    string4 = "'" + datetime + "' , '" + plate_number + "' , '" + type + "' , '" + camera_id + "' , '"+ company_id +"' , '" + best_uuid+ "' , '" + best_region+ "' , " + travel_direction + ",'" + vehicle_make +  "' , '" + vehicle_make_model + "' , '" + vehicle_color +  "' , '" + vehicle_body +  "' , '" + vehicle_year  +  "' ," + is_parked +" ";

    InsertQuery = InsertQuery + " VALUES" + " ( " + string4 + " ) " ;
    console.log(InsertQuery);


    //get site location : start
    let location = await select_location(camera_id);
    //console.log("aaaaaaaaaa: ", location);
    //get site location : end

    let object = {};
    object.found_date = datetime;
    object.plate_number = plate_number;
    object.type = type;
    object.camera_id = camera_id;
    object.company_id = company_id;
    object.best_uuid = best_uuid;
    object.best_region = best_region;
    object.travel_direction = travel_direction;
    object.vehicle_make = vehicle_make;
    object.vehicle_make_model = vehicle_make_model;
    object.vehicle_color = vehicle_color;
    object.vehicle_body_type = vehicle_body;
    object.vehicle_year = vehicle_year;
    object.is_parked = is_parked;

    object.location = location;

    return new Promise(async(resolve, reject) => {
      await ch.query(InsertQuery, { queryOptions: { database: 'default' } }, function(err, result) {
          if (err) {
              console.log("Insert err :: ", err);
          } else {
              console.log("Insert res :: ", result);
              console.log("Insert Completed");
              //console.log("object",object);
              resolve(object)
          }
      });
    });

    //send(res, 200, result)
    //return "Insert Completed";
    // console.log("Insert Completed");
  }
