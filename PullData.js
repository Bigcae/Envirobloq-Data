// this script is a working document
// I'm connecting to iotex pebble data and trying to insert it into mysql database
// I am able to get the pebble data but it is in json and I'm having issues getting it into mysql
// need to figure out how to parse the json data into an array and then insert it into mysql
// at least that is where I'm at currently
//======================================================================================================
//
//

const { request, GraphQLClient } = require("graphql-request");
const { gql } = require('graphql-tag')
const fs = require('fs');

const AbortController = require("node-abort-controller").AbortController;

//Database connection
var mysql = require('mysql');
const { timeStamp, Console, timeLog } = require("console");
const { setTimeout } = require("timers/promises");
const { notEqual } = require("assert");
const { resolve } = require("path");

var con = mysql.createConnection({
  host: 'xxxx.clgks69oznpz.us-east-1.rds.amazonaws.com',
  user: 'xxxx',
  password: 'xxxx!',
  database: 'xxxx'
});

const cron = require("node-cron");
const express = require("express");

const app = express();

// Pebble default W3bstream community node endpoint (mainnet)
const endpoint = "https://pebble.iotex.me/v1/graphql";
const client = new GraphQLClient(endpoint);

// this fetch will get all pebble data for one day.  2022-10-26 that does not eql imei number 351358813281992

let fetchDeviceData = gql`
    query ($IMEI: String!, $TIMESTAMP: Int!) {
        pebble_device_record(
            order_by: {timestamp: desc}, 
            where: {
                _or: [
                    {_and: [ {imei: {_eq: $IMEI}}, {timestamp: {_gt: $TIMESTAMP}}]}
                ]    
            })
            {
            timestamp
            imei
            latitude
            longitude
            temperature
            temperature2
            gas_resistance
            humidity
            pressure
            light
            accelerometer
            gyroscope
            }
    }`;

const controller = new AbortController()
//var myDevices = "351358813357594";
// july 31 at midnight
//var myTimestamp = "1659311999"; 
var myTimestamp = "1675807123"
var myDevices = "351358813357594"

var vIMEI = []
// var vTimeStamp = []

var newData = []


async function main() {
    const vListOfPebbleIDs = await getPebbleIDs();
    
    for (let i =0; i < vListOfPebbleIDs.length; i++) {
        
        console.log("")
        console.log(vListOfPebbleIDs[i].Name, " this is from vListOfPebbleIDS",i);
        let vLastTimestamp = await getLastTimeStamp(vListOfPebbleIDs[i],i);

        if (vLastTimestamp === 0) {
        
            console.log("we have no date sir...")
            myDevices = String(vListOfPebbleIDs[i].PebbleTrackerID);
            myTimestamp = "1659311999";
           
        } else {
        
        myDevices = String(vLastTimestamp[i].imei);
        myTimestamp = String(vLastTimestamp[i].timestamp);

        }

        let result = await client.request(fetchDeviceData, {IMEI: myDevices, TIMESTAMP: myTimestamp});
        let vfetchdata = await cleanUpFetch(result);
        
    }   

    let vlastFunc = await updateTableWeather();
    console.log(" ");
    console.log(vlastFunc); 
    con.end();
}

cron.schedule("*/30 * * * *", function () {
    let ts = Date.now();
    let dateformat = new Date(ts);
    console.log("---------------------");
    console.log("running a task every 10 minutes", dateformat);
    con.connect(function(err) {
        if (err) throw err;
        // if connection is successful
    });
    main();
    //con.end();
  });
  
  app.listen(3001, () => {    
    console.log("application listening.....");
    //main();   
  });

//main();

function getPebbleIDs() {
    return new Promise((resolve, rejects) => {

        // make to connection to the database.
        //con.connect(function(err) {
        //    if (err) throw err;
        //    // if connection is successful
        //});
        
        // then fetch the Name and PebbleTrackerID from the DataProviders table
        SQL = "select Name, PebbleTrackerID from EnviroBloq.DataProviders order by PebbleTrackerID asc limit 200"
        con.query(SQL, function (err, result, fields){
            if (err) throw err;
            console.log("Just got the list of IMEI numbers.......from getPebbleIDs Function");
            resolve(result);
        });  
    });
}


function getLastTimeStamp(listOfImei,i) {
    return new Promise(( resolve, reject) => {
        //var i = 0;
        var vTimeStamp =[];
        //var vFetching = [];
        
        con.query("select timestamp, imei from EnviroBloq.SampleMasterPebbleData where imei= "+listOfImei.PebbleTrackerID+" order by timestamp Desc Limit 1", 
            function (err, result, fields) {
                if (err) throw console.log(err," just had and error");

                if (result.length === 0) {

                    console.log("This is the first for this IMEI: "+ myDevices, myTimestamp+ " to updated in our database");
                    vTimeStamp = 0;

                } else {                       

                    Object.keys(result).forEach(function(key){
                    vTimeStamp[i] = result[key];
                    })

                    myDevices = String(vTimeStamp[i].imei);
                    myTimestamp = String(vTimeStamp[i].timestamp);
                    console.log(myDevices,myTimestamp, " we have data...inside getLastTimeStamp")
                }

            resolve(vTimeStamp)
        });
    });
}

    
function cleanUpFetch(PebbleFetch){
    return new Promise(( resolve, reject) => {
        var values = [];
        var updateValues = [];
        var myresults = PebbleFetch;
        var myparse = JSON.parse(JSON.stringify(myresults));
        newData = myparse.pebble_device_record;
        
        
        newData.forEach((data, index) => {
        //console.log(data.timestamp, data.imei, "New Data Loop");
        });
        
        if (newData.length > 0) {
            for(var c = 0; c < newData.length; c++)
            values.push([
                newData[c].timestamp,
                newData[c].imei, 
                newData[c].latitude, 
                newData[c].longitude, 
                newData[c].temperature,
                newData[c].temperature2,
                newData[c].gas_resistance,
                newData[c].humidity,
                newData[c].pressure,
                newData[c].light,
                newData[c].accelerometer,
                newData[c].gyroscope
            ]);

            console.log("We found "+newData.length+ " records for IMEI: "+myDevices," in CleanUpFetch Function");
            var ldataImei = newData[0].imei;
            updateTableTimestamp(values);

        } else {

            console.log("...........NO RECORDS TO UPDATE FOR.........."+myDevices);
            console.log("");
        };
        resolve(values);
    });
} 





function updateTableTimestamp(values) {
    return new Promise ((resolve,reject) => {
        //console.log(values);
        SQL = "INSERT into EnviroBloq.SampleMasterPebbleData (timestamp, imei, latitude, longitude, temperature, temperature2, gas_resistance, humidity, pressure, light, accelerometer, gyroscope) VALUES ?";
        con.query(SQL,[values]);
        console.log("database updated from updateTableTimestamp function");
        resolve();
    })
}
 
function updateTableWeather() {
    return new Promise ((resolve, reject) => {
        SQL = "UPDATE EnviroBloq.SampleMasterPebbleData SET Fahrenheit2 = Round(((temperature2*1.8)+32),2), Fahrenheit1 = Round(((temperature*1.8)+32),2), Date_Time = from_unixtime(EnviroBloq.SampleMasterPebbleData.timestamp) where Fahrenheit1 is null";
        //SQL = "UPDATE EnviroBloq.SampleMasterPebbleData SET Fahrenheit2 = Round(((temperature2*1.8)+32),2), Fahrenheit1 = Round(((temperature*1.8)+32),2), Date_Time = from_unixtime(EnviroBloq.SampleMasterPebbleData.timestamp)";
        con.query(SQL);
        //console.log("Finished updating Temp and date format for IMEI: "+updateImei);
        // (timestamp/86400)+25569
        resolve("Updating Completed from updateTableWeather function for IMEI: "+ Date(timeStamp))
    }) 
}

    
