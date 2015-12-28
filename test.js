var assert = require("assert");

var SQSToWebhook = require('./index');


describe('sqs-to-webhook api', function() {

    function reportAll(manager) {

        manager.on('starting', function() {
            //            console.log('starting', arguments)
            console.log('starting')
        });
        manager.on('request', function() {
            //            console.log('request', arguments)
            console.log('request')
        });
        manager.on('response', function() {
            //            console.log('response', arguments)
            console.log('response')
        });
        manager.on('polling', function() {
            //            console.log('polling', arguments)
            console.log('polling')
        });
        manager.on('receivemessage', function() {
            //            console.log('receivemessage', arguments)
            console.log('receivemessage')
        });
        manager.on('messagereceived', function() {
            //            console.log('messagereceived', arguments)
            console.log('messagereceived')
        });
        manager.on('messagesreceived', function() {
            //            console.log('messagesreceived', arguments)
            console.log('messagesreceived')
        });
        manager.on('deletemessage', function() {
            //            console.log('deletemessage', arguments)
            console.log('deletemessage')
        });
        manager.on('queuemessage', function() {
            //            console.log('queuemessage', arguments)
            console.log('queuemessage')
        });
        manager.on('queuemessagecomplete', function() {
            //            console.log('queuemessagecomplete', arguments)
            console.log('queuemessagecomplete')
        });
        manager.on('changevisibility', function() {
            //            console.log('changevisibility', arguments)
            console.log('changevisibility')
        });
        manager.on('messagecomplete', function() {
            //            console.log('messagecomplete', arguments)
            console.log('messagecomplete')
        });

        reportErrors(manager);
    }


    function reportErrors(manager) {


        manager.on('changevisibilityerror', function() {
            console.log('changevisibilityerror', arguments)
        });
        manager.on('queuemessageerror', function() {
            console.log('queuemessageerror', arguments)
        });
        manager.on('deletemessageerror', function() {
            console.log('deletemessageerror', arguments)
        });
        manager.on('receivemessageerror', function() {
            console.log('receivemessageerror', arguments)
        });
        manager.on('requesterror', function() {
            console.log('requesterror', arguments)
        });


    }


    beforeEach(function(done) {
        this.timeout(100000);
        var manager = new SQSToWebhook();

        reportErrors(manager);

        function readUntilPurge(callback) {
            //purge the message queue
            manager.getQueueSize(process.env.AWS_SQS_QUEUE_URL, function(err, count) {
//                console.log("queue size", count);

                if (!count) {
  //                  console.log("queue emtpy");
                    return done();
                }

                manager.receiveMessages(process.env.AWS_SQS_QUEUE_URL, 10, function(err, messages) {
    //                console.log('deleting', messages.length);
                    messages.map(function(msg) {
                        manager.deleteMessage(msg);
                    });

                    setTimeout(function() {
                        readUntilPurge();
                    }, 1000);

                });


            });

        }
        readUntilPurge();

    });

    it('can add a message and process it', function(done) {
        this.timeout(25000);

        var testId = Math.random().toString();

        var manager = new SQSToWebhook();
        //reportAll(manager);
        manager.start({
            queueUrl: process.env.AWS_SQS_QUEUE_URL,
            webhookUrl: 'succeed'
        });
        manager.queueMessage({
            queueUrl: process.env.AWS_SQS_QUEUE_URL,
            Body: testId
        });

        //watch for the messagecomplete
        manager.on('messagecomplete', function(p) {
            //console.log(p);
            assert.ok(p.message);
            assert.ok(p.message.Body);
            if (p.message.Body == testId) {
                manager.stop(done);
            }
        });

    });


    it('can add a message to google and process it', function(done) {
        this.timeout(25000);

        var testId = Math.random().toString();

        var manager = new SQSToWebhook();
        //      reportAll(manager);
        manager.start({
            queueUrl: process.env.AWS_SQS_QUEUE_URL,
            webhookUrl: 'https://www.google.com'
        });
        manager.queueMessage({
            queueUrl: process.env.AWS_SQS_QUEUE_URL,
            Body: testId
        });

        //watch for the messagecomplete
        manager.on('messagecomplete', function(p) {
            //console.log(p);
            assert.ok(p.message);
            assert.ok(p.message.Body);
            if (p.message.Body == testId) {
                manager.stop(done);
            }
        });

    });


    it('failed change visibility will result in a new message being inserted', function(done) {
        this.timeout(25000);

        var testId = Math.random().toString();

        var manager = new SQSToWebhook();
        //reportAll(manager);
        manager.start({
            queueUrl: process.env.AWS_SQS_QUEUE_URL,
            webhookUrl: 'fail'
        });
        manager.queueMessage({
            queueUrl: process.env.AWS_SQS_QUEUE_URL,
            Body: testId
        });

        var originalMessageId = null;
        var errored = false;

        manager.on('changevisibility', function(p) {
            assert.ok(p.message);
            assert.ok(p.message.Body);
            if (p.message.Body == testId) {
                //change the visibility so it will fail
                originalMessageId = p.message.MessageId;
                p.params.VisibilityTimeout = 43200;
                manager.options.webhookUrl = 'succeed';
            }
        });

        manager.on('changevisibilityerror', function(p) {
            assert.ok(p.message);
            assert.ok(p.message.Body);
            errored = true;
        });

        manager.on('queuemessage', function(p) {
            assert.ok(p.params.DelaySeconds);
            p.params.DelaySeconds = 1;
        });

        //watch for the messagecomplete
        manager.on('messagecomplete', function(p) {
            //console.log(p);
            assert.ok(p.message);
            assert.ok(p.message.Body);
            assert.ok(originalMessageId !== p.message.MessageId);
            if (errored) {
                manager.stop(done);
            }
        });

    });

    it('failed request will be placed back into the queue', function(done) {
        this.timeout(25000);

        var testId = Math.random().toString();

        var manager = new SQSToWebhook();
        //reportAll(manager);
        manager.start({
            queueUrl: process.env.AWS_SQS_QUEUE_URL,
            webhookUrl: 'fail'
        });
        manager.queueMessage({
            queueUrl: process.env.AWS_SQS_QUEUE_URL,
            Body: testId
        });

        var switched = false;

        manager.on('changevisibility', function(p) {
            p.params.VisibilityTimeout = 1;
            manager.options.webhookUrl = 'succeed';
            switched = true;
        });

        //watch for the messagecomplete
        manager.on('messagecomplete', function(p) {
            //console.log(p);
            assert.ok(p.message);
            assert.ok(p.message.Body);
            if (switched && p.message.Body == testId) {
                manager.stop(done);
            }
        });

    });

    /**/

});
