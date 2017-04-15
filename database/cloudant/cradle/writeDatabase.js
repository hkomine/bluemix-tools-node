var fs = require('fs');
var csv = require('ya-csv');
var async = require('async');
var cradle = require('cradle');
var argv = require('argv');
var services = require('./vcap.json');

argv.option([ {
	name : 'input',
	short : 'i',
	type : 'string',
	description : 'input csv filename',
	example : "'script --input=<csv filename>' or 'script -i <csv filename>'"
}, {
	name : 'database',
	short : 'n',
	type : 'string',
	description : 'configuration id',
	example : "'script --database=<database name>' or 'script -n <database name>'"
}, {
	name : 'debug',
	short : 'd',
	type : 'boolean',
	description : 'enable debug output for script',
	example : "'script --debug=true' or 'script -d true' or 'script -d'"
} ]);

// get arguments
var args = argv.run();
var input = args.options.input;
var database = args.options.database;
var debug = args.options.debug;

if (!input || !database){
	console.error('Required argument missing.');
	console.error("'script --help' or 'script -h' to show help.");
	process.exit();
}

// defaults for cloudant connection
var service_url = '<service_url>';
var service_username = '<service_username>';
var service_password = '<service_password>';
var service_host = '<service_host>';
var service_port = '<service_port>';

// read vcap.json that was extracted from VCAP_SERVICES of Bluemix app

var service_name = 'cloudantNoSQLDB';
if (services[service_name]) {
    var credentials = services[service_name][0].credentials;
    service_url = credentials.url;
    service_username = credentials.username;
    service_password = credentials.password;
    service_host = credentials.host;
    service_port = credentials.port;
} else {
    console.log('The service ' + service_name + ' is not in vcap.json');
}

if (debug) {
	console.log('service_url = ' + service_url);
	console.log('service_username = ' + service_username);
	console.log('service_password = ' + new Array(service_password.length).join("X"));
	console.log('service_host = ' + service_host);
	console.log('service_port = ' + service_port);
}

// set options for connecting database
// see http://www.ibm.com/developerworks/jp/cloud/library/j_cl-bluemix-nodejs-app/
var options = {
	cache : true,
	raw : false,
	secure : true,
	auth : {
		username : service_username,
		password : service_password
	}
};
var db = new(cradle.Connection)(service_host, service_port, options).database(database);

// test if the database exists
db.exists(function (err, exists) {
    if (err) {
      console.log('error', err);
    } else if (exists) {
      if (debug) {
          console.log('the database exists.');
      }
      
      var reader = csv.createCsvFileReader(input,  {columnsFromHeader: true});
      reader.on('data', function(data) {
          if (debug) {
        	  console.log("[reader.on] record = ",JSON.stringify(data, null, 2));
          }
          db.save(data, function (err, res) {
        	  if(err) {
        		  console.error(err);
        	  }
          });
      });
      
    } else {
      console.error('database does not exist.');
    }
  });
