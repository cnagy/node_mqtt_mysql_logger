var mqtt = require('mqtt');
var mysql = require('mysql');

var MQTT_TOPIC = 'shellies/#';
const BROKER_URL = process.env.BROKER_URL;
const BROKER_USER = process.env.BROKER_USER;
const BROKER_PASS = process.env.BROKER_PASS;

const DATABASE_URL = process.env.DATABASE_URL;
const DATABASE_USER = process.env.DATABASE_USER;
const DATABASE_PASS = process.env.DATABASE_PASS;

var options = {
	clientId: 'NodeMQTT',
	port: 1883,
	username: BROKER_USER,
	password: BROKER_PASS,	
	keepalive : 60
};

var con = mysql.createConnection({
	host: DATABASE_URL,
	user: DATABASE_USER,
	password: DATABASE_PASS,
	database: "mqttlogs"
});

con.connect(function(err) {
  if (err) throw err;
  	
});

var client  = mqtt.connect(BROKER_URL, options);
client.on('connect', mqtt_connect);
client.on('reconnect', mqtt_reconnect);
client.on('error', mqtt_error);
client.on('message', mqtt_messsageReceived);
client.on('close', mqtt_close);

function mqtt_connect() {
    client.subscribe(MQTT_TOPIC, mqtt_subscribe);
};

function mqtt_subscribe(err) {
    if (err) {console.log(err);}
};

function mqtt_reconnect(err) {
    client  = mqtt.connect(BROKER_URL, options);
};

function mqtt_error(err) {

};

function after_publish() {

};

function mqtt_messsageReceived(topic, message, packet) {
	var message_str = message.toString();
	message_str = message_str.replace(/\n$/, '');

	var topicArr = topic.split('/');

	if (topic.includes('relay') && (topicArr[4] == 'power' || topicArr[4] ==  'energy') && parseInt(message_str) > 0) {
		
		const sql = "SELECT id from devices WHERE device_name LIKE ?";
		con.query(sql, [topicArr[1]], function (err, result, fields) {
			if (err) {console.log(err);}
			
		    if (result.length) {
		    	if (topicArr[4] == 'power') {
		    		const sql = "INSERT INTO power_events (device_id, power) VALUES (?, ?)";
		    		con.query(sql, [result[0].id, parseFloat(message_str)], function (err, result, fields) {
			    		if (err) {console.log(err);}
			    	});
		    	} else if (topicArr[4] ==  'energy') {
		    		const sql = "SELECT MAX(energy) AS energy FROM energy_events WHERE device_id = ?";
					con.query(sql, [result[0].id], function (er, res, fi) {
						if (typeof(res[0].energy) != "undefined" && res[0].energy == parseInt(message_str)) {
							
						} else {
							const sql = "INSERT INTO energy_events (device_id, energy) VALUES (?, ?)";
				    		con.query(sql, [result[0].id, parseInt(message_str)], function (err, result, fields) {
					    		if (err) {console.log(err);}
					    	});
						}
		    		});
		    	}
		    } else {
		    	const sql = "INSERT INTO devices (device_name) VALUES (?)";
		    	con.query(sql, [topicArr[1]], function (err, result, fields) {
		    		if (err) {console.log(err);}
		    	});
		    }
	  	});
	}
};

function mqtt_close() {

};


