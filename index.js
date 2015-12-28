var async = require('async');
var moment = require('moment');
var request = require('request');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var AWS = require('aws-sdk');


var sqsconfig = {
    apiVersion: '2012-11-05',
    region: process.env.AWS_REGION,
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || undefined,
    secretAccessKey: process.env.AWS_SECRET_KEY_ID || undefined,
}

var sqs = new AWS.SQS(sqsconfig);

function SQSToWebhook(options) {

    EventEmitter.call(this);

    //trackk the total number of messages in flight at 
    //the same time so we don't run out of resources
    this.totalMessagesInFlight = 0;

    this.options = options || {};

}

util.inherits(SQSToWebhook, EventEmitter);


var start = SQSToWebhook.prototype.start = function start(options, done) {
    done = done || function() {};
    var self = this;

    self.receiveMessagesCurrentlyExecuting = 0;
    self.shouldStop = false;

    self.options = {
        maxMessagesInFlight: options.maxMessagesInFlight || self.options.maxMessagesInFlight || 1,
        queueUrl: options.queueUrl || self.options.queueUrl,
        webhookUrl: options.webhookUrl || self.options.webhookUrl || "success",
        headers: options.headers || self.options.headers || {},
        linearSecondDelayPerSecondSinceCreation: options.linearSecondDelayPerSecondSinceCreation || self.options.linearSecondDelayPerSecondSinceCreation || 1.0
    }

    if (!self.options.queueUrl) throw new Error("Must provide a queueUrl.");
    if (!self.options.webhookUrl) throw new Error("Must provide a webhookUrl.");
    if (typeof self.options.queueUrl === "string") self.options.queueUrl = [self.options.queueUrl];

    self.emit('starting', self.options);

    self.options.queueUrl.map(function(queueUrl) {
        self.tendril(queueUrl);
    });

}


var stop = SQSToWebhook.prototype.stop = function stop(done) {
    done = done || function() {};
    var self = this;

    self.shouldStop = true;
    if (self.receiveMessagesCurrentlyExecuting == 0) return done();

    setImmediate(function() {
        self.stop(done);
    });

}

var callWebhook = SQSToWebhook.prototype.callWebhook = function callWebhook(message, done) {
    done = done || function() {};
    var self = this;


    var params = {
        url: message.Body.webhookUrl || self.options.webhookUrl,
        headers: self.options.headers,
        method: "POST",
        json: true,
        body: message.Body
    };


    self.emit('request', {
        message: message,
        params: params,
    });


    if (params.url == "succeed") {

        self.emit('response', {
            message: message,
            body: message.Body
        });

        return done(null, null);
    }

    if (params.url == "fail") {

        self.emit('requesterror', {
            message: message,
            params: params,
            err: new Error("simulated failure"),
        });

        return done(new Error("simulated failure"));
    }



    request(params, function(err, response, body) {
        if (body && body.message) err = new Error(body.message);

        if (err) {

            self.emit('requesterror', {
                message: message,
                params: params,
                err: err,
            });

            return done(err);

        }
        self.emit('response', {
            message: message,
            body: body
        });

        return done(null, body);

    });

}


var tendril = SQSToWebhook.prototype.tendril = function tendril(queueUrl, done) {
    done = done || function() {};

    var self = this;

    var headroom = self.options.maxMessagesInFlight - this.totalMessagesInFlight;

    if (!headroom) {
        //we are out of headroom, try again in 1 second
        !self.shouldStop && setTimeout(function() {
            self.tendril(queueUrl, done);
        }, 1000);
        return;
    }


    self.emit('polling', {
        queueUrl: queueUrl,
        headroom: headroom
    });

    //chack to make sure there are not multiple threads running
    if (self.receiveMessagesCurrentlyExecuting) {
        console.log("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEERROR multiple threads running");
    }

    self.receiveMessagesCurrentlyExecuting += 1;
    this.receiveMessages(queueUrl, headroom, function(err, messages) {

        async.each(messages, function(message, done) {

            self.totalMessagesInFlight += 1;

            self.callWebhook(message, function(err, body) {

                if (err) {
                    self.changeMessageVisibility(message);
                } else {
                    self.emit('messagecomplete', {
                        message: message,
                    });
                    self.deleteMessage(message);
                }

                self.totalMessagesInFlight -= 1;


                return;

            });

        });

        self.receiveMessagesCurrentlyExecuting -= 1;

        //try again, waiting a bit if there was an error or no messages
        !self.shouldStop && setTimeout(function() {
            self.tendril(queueUrl, done)
        }, err ? 1000 : 0);



    });


}




var deleteMessage = SQSToWebhook.prototype.deleteMessage = function deleteMessage(message, done) {
    done = done || function() {};
    var self = this;
    if (message.deleted) return done(null, message);

    var params = {
        QueueUrl: message.queueUrl,
        ReceiptHandle: message.ReceiptHandle,
    };

    self.emit('deletemessage', {
        message: message,
        params: params,
    });

    sqs.deleteMessage(params, function(err) {
        if (err) {
            self.emit('deletemessageerror', {
                message: message,
                params: params,

                err: err
            });
            return done(err);
        }
        message.deleted = true;
        return done(null, message);
    });

}


var queueMessage = SQSToWebhook.prototype.queueMessage = function queueMessage(message, done) {
    done = done || function() {};
    var self = this;

    var params = {
        QueueUrl: message.queueUrl,
        MessageBody: JSON.stringify(message.Body),
    };

    if (message.Attributes && message.Attributes.ApproximateFirstReceived) {
        params.DelaySeconds = Math.abs(moment().diff(message.Attributes.ApproximateFirstReceived, 'seconds'));
    }

    self.emit('queuemessage', {
        message: message,
        params: params,
    });

    sqs.sendMessage(params, function(err, result) {

        if (err) {
            self.emit('queuemessageerror', {
                message: message,
                params: params,
                err: err
            });
            //wait for a second and try again
            !self.shouldStop && setTimeout(function() {
                self.queueMessage(message, done)
            }, 1000)
            return;
        }

        self.emit('queuemessagecomplete', {
            message: message,
            params: params,
            result: result,
        });

        return done(null, message);

    });


}



var changeMessageVisibility = SQSToWebhook.prototype.changeMessageVisibility = function changeMessageVisibility(message, done) {
    done = done || function() {};
    var self = this;

    var params = {
        QueueUrl: message.queueUrl,
        ReceiptHandle: message.ReceiptHandle,
        VisibilityTimeout: 1
    };

    if (message.Attributes && message.Attributes.ApproximateFirstReceived) {
        params.VisibilityTimeout = 1 + Math.abs(moment().diff(message.Attributes.ApproximateFirstReceived, 'seconds'));
    }

    self.emit('changevisibility', {
        params: params,
        message: message,
    });

    sqs.changeMessageVisibility(params, function(err, data) {
        if (err) {
            //there was an error changing the visibility, probably we are hittim some limits
            self.emit('changevisibilityerror', {
                params: params,
                message: message,
                err: err
            });
            self.queueMessage(message, function() {
                //queuemessage will keep trying until it succeeds
                //then we can delete this one
                self.deleteMessage(message, done);
            });
            return;
        }

        return done(null, message);
    });

}









var getQueueSize = SQSToWebhook.prototype.getQueueSize = function getQueueSize(queueUrl, done) {
    done = done || function() {};
    var self = this;

    var params = {
        QueueUrl: queueUrl,
        AttributeNames: [
            'ApproximateNumberOfMessages',
            'ApproximateNumberOfMessagesNotVisible',
            'ApproximateNumberOfMessagesDelayed',
        ]
    };
    sqs.getQueueAttributes(params, function(err, data) {
        if (err) return done(err);
        return done(null, (Number(data.Attributes.ApproximateNumberOfMessages) || 0) + (Number(data.Attributes.ApproximateNumberOfMessagesDelayed) || 0) + (Number(data.Attributes.ApproximateNumberOfMessagesNotVisible) || 0));
    });

}



var receiveMessages = SQSToWebhook.prototype.receiveMessages = function receiveMessages(queueUrl, numberOfMessages, done) {
    done = done || function() {};
    var self = this;


    var params = {
        QueueUrl: queueUrl,
        MaxNumberOfMessages: Math.min(numberOfMessages, 10),
        VisibilityTimeout: 10,
        WaitTimeSeconds: 1,
        AttributeNames: ['ApproximateFirstReceiveTimestamp'],
    };

    self.emit('receivemessage', {
        params: params,
    });

    sqs.receiveMessage(params, function(err, data) {

        data = data || {
            Messages: []
        };

        data.Messages = data.Messages || [];

        data.Messages = data.Messages.map(function(msg) {
            msg.queueUrl = queueUrl;
            msg.Body = JSON.parse(msg.Body);
            msg.Attributes.ApproximateFirstReceived = new Date(0);
            msg.Attributes.ApproximateFirstReceived.setUTCSeconds(msg.Attributes.ApproximateFirstReceiveTimestamp / 1000);
            return msg;
        });

        if (err) {

            self.emit('receivemessageerror', {
                params: params,
                err: err
            });

            return done(err);

        }

        self.emit('messagesreceived', {
            params: params,
            messages: data.Messages,
            messageCount: data.Messages.length,
        });

        return done(null, data.Messages);

    });
}


module.exports = SQSToWebhook;
