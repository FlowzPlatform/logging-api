const { json, send, createError, sendError } = require('micro');
const { router, get, post } = require('microrouter')
const ClickHouse = require("@apla/clickhouse");
console.log(ClickHouse);

const hello = async(req, res) => {

    console.log('hello.......');

    send(res, 200, "hi..")
}

const logData = async(req, res) => {

    const body = await json(req)
    console.log('this is logdata :: ', body.ip);
    console.log(body);
    console.log(JSON.stringify(body));

    let ch = new ClickHouse({ host: 'localhost' });
    //let dbQuery = "INSERT INTO cdb VALUES (2004,'2004-07-18','{\"key1\":\"val1\"}')";
    let dbQuery = "INSERT INTO cdb VALUES (2004,'2004-07-18','" + JSON.stringify(body) + "')";
    ch.query(dbQuery, { queryOptions: { database: 'default' } }, function(err, result) {
        //assert (!err, err);
        if (err) {
            console.log("err :: ", err);
        } else {
            console.log("res :: ", result);
        }
        //done ();
    });

    send(res, 200, "hi logData..")
}

module.exports = router(
    //post('/hellopost', hellopost),
    get('/hello/:who', hello),
    post('/logdata', logData)
    //get('/*', notfound)
)

/*
module.exports = async(req, res) => {

    req = await json(req)

    console.log(req.ip);

    res.end('Welcome to Micro')
} */