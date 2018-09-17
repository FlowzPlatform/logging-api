const { json, send, createError, sendError } = require('micro');
const { router, get, post } = require('microrouter');
const ClickHouse = require("@apla/clickhouse");
const ch = require('./db');
const url = require('url');
const _= require('lodash');
const cors = require('micro-cors')();

console.log(ClickHouse);
console.log('=============================')

const hello = async (req, res) => {
  send(res,200,"Hello User...!!")
};

const create_with_insert = async(req, res) => {

    //console.log('into create with insert');
    let body = await json(req);
    var TableName = body.schema_name;
    var SchemaDetail = body.schema;
    var data = body.data;
    if(TableName == null || SchemaDetail == null || data == null){

      let result_json = {};
      result_json.response = "Please provide schema_name, schema and data.";
      return result_json;
    };

    let result_string =  await create(req, res, 1);

    let result_json = {};
    result_json.response = result_string;

    send(res, 200, result_json);
};

const create = async(req, res, isInsert=0) => {

      console.log('Create With Insert :: ', isInsert);

      let body = await json(req);

      var TableName = body.schema_name;
      console.log('Create tablename :', TableName);

      let show_result;
      try{
        show_result = await show_table(TableName, res);
        console.log("Table Exists result :", show_result, show_result.length);
        // console.log("Table Exists :", show_result.length);
      }
      catch(err){
        console.log("show table error", err);
      }

      if(show_result.length > 0){
        let result_string = "Table already exists...!!";
        if(isInsert == 0){
            let result_json = {};
            result_json.response = result_string;
            return(result_json);
        }
        else{
            return(result_string);
        }
      }

      const SchemaDetail = body.schema;

      let Fields = Object.keys(SchemaDetail);
      let Datatypes = Object.values(SchemaDetail);

      let datatypes = [];
      datatypes["string"] = "String";
      datatypes["int"] = "UInt64";
      datatypes["float"] = "Float64";



      let createQuery = "CREATE TABLE IF NOT EXISTS `" + TableName + "`";
      let string1 =   " `logDateTime` DateTime";

      Fields.forEach(function(element){

        // console.log(datatypes[SchemaDetail[element]]);
        //string1 = string1 + ", `" + element +  "` " + SchemaDetail[element] ;
        string1 = string1 + ", `" + element +  "` " + datatypes[SchemaDetail[element]] ;
      });

      createQuery = createQuery + " ( " + string1 + " ) " + "ENGINE = Log " ;
      // console.log(createQuery);

      // START : To create Table logtable if not exists
      // let CreateQuery = "CREATE TABLE IF NOT EXISTS `logtable`  ( `logDate` Date, `logDateTime` DateTime, `ip` String, `location` String, `pagename` String, `username` String, `referrer` String ) ENGINE = MergeTree(logDate, (logDate), 8192)";

      //assert (!err, err);
      let result_string = "";
      return new Promise(async(resolve, reject) => {
      let response = await ch.query(createQuery, { queryOptions: { database: 'default' } },async  function(err, result) {
          if (err) {
              console.log("Create error :: ", err);
              result_string = err;
          } else {
              console.log("Create result :: ", result);
              result_string = result_string + " Create Completed! ";
              //console.log("isInsert ::", isInsert);
              if(isInsert == 1)
              {
                console.log('create complete now insert data...!');
                let response = await insert(req, res);
                result_string = result_string + " Insert Completed! ";
                resolve(result_string);
              }
              let result_json = {};
              result_json.response = result_string;
              resolve(result_json);
          }
      });
    });
      // END : To create Table logtable if not exists
};

const insert = async(req, res) => {
    let body = await json(req)

    var TableName = body.schema_name;

    console.log("Insert Tablename :", TableName);
    const data = body.data;

  let colnames = Object.keys(data);
  let colvalues = Object.values(data);

  //let date = new Date().toISOString().split('T')[0];
  //console.log(date);
  let datetime =  new Date().toISOString().split('T')[0] + " " + new Date().toLocaleTimeString() ;
  //console.log(datetime);

  //let InsertQuery = "INSERT INTO logtable VALUES ( '" + new Date().toISOString().split('T')[0] + "','" + datetime + "','"+ ip + "','"+ location + "','" + pagename + "','" + username + "','" + referrer + "')";

  let InsertQuery = "INSERT INTO " + TableName;

  let string3 =   " `logDateTime`";
  colnames.forEach(function(element){
    string3 = string3 + ", `" + element +  "` ";
  });

  let string4 =   "'" + datetime + "'";
  colvalues.forEach(function(element){
    //console.log(typeof(element));
    if(typeof(element) == "string"){
      string4 = string4 + ", '" + element +  "' ";
    }
    else{
      string4 = string4 + ", " + element +  " ";
    }
  });

  InsertQuery = InsertQuery + " ( " + string3 + " ) " + "VALUES" + " ( " + string4 + " ) " ;
  // console.log(InsertQuery);


  let result_string ="";
  return new Promise(async(resolve, reject) => {
  let response = await ch.query(InsertQuery, { queryOptions: { database: 'default' } }, function(err, result) {
      if (err) {
          console.log("Insert err :: ", err);
          result_string = err;
      } else {
          console.log("Insert res :: ", result);
          result_string = "Insert completed...!!";
      }
      let result_json = {};
      result_json.response = result_string;
      resolve(result_json);
    });
  });

};

const select = cors(async(req, res) => {

    var url_parts = url.parse(req.url, true);
    // console.log(url_parts);

    var query = url_parts.query;
    //console.log(query);

    var keys = Object.keys(query);
    var values = Object.values(query);
    //console.log(keys);
    //console.log(values);

    selectQuery = "SELECT * from alpr_group " ;

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
  return new Promise(async(resolve, reject) => {
    await stream.on ('end', function () {
       // all rows are collected, let's verify count
      resolve(1)
    });
  });
};

const select_table = cors(async(req, res) => {

    //console.log(req.params);

    var tablename = req.params.tbl;

    selectQuery = "SELECT * from " + tablename + " FORMAT JSON";

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

const show_table = cors(async(TableName, res) => {

    console.log("Tablename to check Exists or not :", TableName)

    selectQuery = "SHOW tables LIKE '" + TableName + "' FORMAT JSON" ;

    //console.log(selectQuery);
    //console.log("ch",ch);
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
          console.log('show_table error :', err);
        });

      await fetchData2(stream);
      return rows;
});

module.exports = router(
    get('/', hello),
    post('/create', create),
    post('/insert', insert),
    post('/create-with-insert', create_with_insert),
    get('/getdata', select),
    get('/fetchdata/:tbl', select_table)
)
