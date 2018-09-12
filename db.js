const ClickHouse = require("@apla/clickhouse");

const { dbhost, dbport } = require('./config');

//console.log("dbhost ",dbhost, "dbport ",dbport );

let  ch = new ClickHouse({ host: dbhost, port: dbport});
//console.log("ch ",ch);

module.exports = ch;
