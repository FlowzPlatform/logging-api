var NATS = require('nats');
const ClickHouse = require("@apla/clickhouse");

var servers = ['nats://172.16.61.20:4222'];
var nats = NATS.connect({'servers': servers});

const ch = new ClickHouse({ host: 'localhost', port: 8123});

// currentServer is the URL of the connected server.
console.log("Connected to " + nats.currentServer.url.host);

// Simple Subscriber
nats.subscribe('publish-data', function(msg,res) {
  console.log('Received a message: ');
  console.log('Inserting into clickhouse: ');
  try{
    inserttest(msg,res);
  }
  catch(error){
    console.log(error);
  }
});

const inserttest = async(req, res) => {

      // let body = await json(req);
      //let body = req;
      let body = JSON.parse(req);


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
            console.log("Insert Completed");
        }
    });
    //send(res, 200, result)
    //return "Insert Completed";
    // console.log("Insert Completed");
  }
